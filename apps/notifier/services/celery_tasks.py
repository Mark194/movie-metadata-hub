import logging

from celery import Celery
from jinja2 import Template as JinjaTemplate
from sqlalchemy import func

from db.sync_database import SyncSessionLocal
from models.notify import Notification, NotificationStatus, Template
from external_services.user_service import UserService
from external_services.senders import send_email, send_sms, send_push
from common import get_settings, get_logger

settings = get_settings()

celery_app = Celery(
    'notification_worker',
    broker=settings.notify.celery_url,
    backend=settings.notify.celery_backend,
)
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

logger = get_logger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def send_notification(self, user_id, template_id=None, subject=None, body=None,
                      type_=None, context=None, notification_id=None):
    """
    Отправляет уведомление.
    Если notification_id передан - обновляем существующую запись.
    Иначе создаём новую запись в БД.
    """
    db = SyncSessionLocal()
    try:
        # 1. Получить данные пользователя (внешний сервис)
        user_data = UserService.get_user(user_id)
        if not user_data:
            raise ValueError(f'User {user_id} not found')

        # 2. Определить финальные subject, body и type
        final_subject = subject
        final_body = body
        final_type = type_

        if template_id:
            template = db.query(Template).filter(Template.id == template_id).first()
            if not template:
                raise ValueError(f'Template {template_id} not found')
            final_type = template.type.value
            context = context or {}
            context['name'] = user_data.get('name', 'User')
            jinja_subj = JinjaTemplate(template.subject_template)
            jinja_body = JinjaTemplate(template.body_template)
            final_subject = jinja_subj.render(**context)
            final_body = jinja_body.render(**context)

        if not final_type:
            raise ValueError('Notification type must be provided')

        # 3. Если notification_id передан - обновляем существующую запись
        if notification_id:
            notif = db.query(Notification).filter(Notification.id == notification_id).first()
            if not notif:
                raise ValueError(f'Notification {notification_id} not found')
            # обновляем поля (на случай, если они изменились)
            notif.subject = final_subject
            notif.body = final_body
            notif.type = final_type
            notif.status = NotificationStatus.PENDING
        else:
            # Создаём новую запись
            notif = Notification(
                user_id=user_id,
                template_id=template_id,
                subject=final_subject,
                body=final_body,
                type=final_type,
                status=NotificationStatus.PENDING,
            )
            db.add(notif)
        db.commit()
        if not notification_id:
            db.refresh(notif)

        # 4. Отправить через соответствующий канал
        success = False
        error_msg = None
        try:
            if final_type == 'email':
                success = send_email(user_data['email'], final_subject, final_body)
            elif final_type == 'sms':
                success = send_sms(user_data['phone'], final_body)
            elif final_type == 'push':
                success = send_push(user_id, final_subject, final_body)
            else:
                raise ValueError(f'Unsupported type: {final_type}')
        except Exception as e:
            success = False
            error_msg = str(e)

        # 5. Обновить статус
        if success:
            notif.status = NotificationStatus.SENT
            notif.sent_at = func.now()
        else:
            notif.status = NotificationStatus.FAILED
            notif.error = error_msg or 'Unknown error'
        db.commit()

        if not success:
            # Ошибка отправки - ретраим, но уже с notification_id
            raise Exception(f'Send failed: {error_msg}')

        return {'notification_id': notif.id, 'status': 'sent'}

    except ValueError as e:
        # Не ретраим ошибки валидации (пользователь не найден, шаблон не найден, тип не указан и т.п.)
        logger.error(f'Validation error in send_notification: {e}')
        # Если есть notification_id, обновляем статус на FAILED
        if notification_id:
            try:
                notif = db.query(Notification).filter(Notification.id == notification_id).first()
                if notif:
                    notif.status = NotificationStatus.FAILED
                    notif.error = str(e)
                    db.commit()
            except Exception:
                pass
        # Не ретраим
        return {'status': 'failed', 'error': str(e)}
    except Exception as e:
        db.rollback()
        # При ретрае передаём notification_id, если он уже есть
        retry_kwargs = {'notification_id': notification_id} if notification_id else {}
        # Передаём все остальные аргументы, кроме self
        kwargs = {k: v for k, v in self.request.kwargs.items() if k != 'notification_id'}
        kwargs.update(retry_kwargs)
        raise self.retry(exc=e, kwargs=kwargs)
    finally:
        db.close()


@celery_app.task
def broadcast_notification(template_id: int, context: dict = None,
                           type_: str = None, subject: str = None, body: str = None):
    """
    Отправка всем пользователям через пагинацию и батчи.
    """
    batch_size = 100  # количество user_id в одной задаче
    user_ids_batch = []

    for user in UserService.iter_users(page_size=100):
        user_ids_batch.append(user["id"])
        if len(user_ids_batch) >= batch_size:
            # Отправляем батч в отдельную таску
            send_batch_notifications.delay(
                user_ids_batch.copy(),
                template_id=template_id,
                context=context,
                type_=type_,
                subject=subject,
                body=body
            )
            user_ids_batch.clear()

    # Остаток
    if user_ids_batch:
        send_batch_notifications.delay(
            user_ids_batch,
            template_id=template_id,
            context=context,
            type_=type_,
            subject=subject,
            body=body
        )


@celery_app.task
def send_batch_notifications(user_ids: list, template_id: int = None,
                             context: dict = None, type_: str = None,
                             subject: str = None, body: str = None):
    """
    Обрабатывает батч пользователей, для каждого вызывает send_notification.
    """
    for uid in user_ids:
        send_notification.delay(
            user_id=uid,
            template_id=template_id,
            context=context,
            type_=type_,
            subject=subject,
            body=body
        )