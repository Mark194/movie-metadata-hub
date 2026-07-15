from celery import shared_task
from jinja2 import Template as JinjaTemplate

from db.postgres import get_db
from common import get_logger
from external_services.user_service import UserService
from external_services.senders import send_email, send_sms, send_push
from models.notify import Notification, NotificationStatus, NotificationType, Template
from sqlalchemy import func

logger = get_logger(__name__)


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def send_notification(self, user_id: int, template_id: int = None,
                      subject: str = None, body: str = None,
                      type_: str = None, context: dict = None):
    db = get_db()
    try:
        # 1. Получить данные пользователя
        user_data = UserService.get_user(user_id)
        if not user_data:
            raise ValueError(f'User {user_id} not found')

        # 2. Определить тип и финальные текст/тему
        final_subject = subject
        final_body = body
        final_type = type_

        if template_id:
            template = db.query(Template).filter(Template.id == template_id).first()
            if not template:
                raise ValueError(f'Template {template_id} not found')
            final_type = template.type.value  # переопределяем тип из шаблона
            # Рендерим шаблон
            context = context or {}
            context['name'] = user_data.get('name', 'User')
            # Можно добавить другие поля из user_data при необходимости
            jinja_subj = JinjaTemplate(template.subject_template)
            jinja_body = JinjaTemplate(template.body_template)
            final_subject = jinja_subj.render(**context)
            final_body = jinja_body.render(**context)

        # Если тип не определён, ошибка
        if not final_type:
            raise ValueError('Notification type must be provided')

        # 3. Сохранить запись в БД со статусом PENDING
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
        db.refresh(notif)

        # 4. Отправить через соответствующий канал
        success = False
        error_msg = None
        try:
            if final_type == NotificationType.EMAIL.value:
                success = send_email(user_data['email'], final_subject, final_body)
            elif final_type == NotificationType.SMS.value:
                success = send_sms(user_data['phone'], final_body)
            elif final_type == NotificationType.PUSH.value:
                success = send_push(user_id, final_subject, final_body)
            else:
                raise ValueError(f'Unsupported type: {final_type}')
        except Exception as e:
            success = False
            error_msg = str(e)
            logger.exception(f'Send failed for notification {notif.id}')

        # 5. Обновить статус
        if success:
            notif.status = NotificationStatus.SENT
            notif.sent_at = func.now()
        else:
            notif.status = NotificationStatus.FAILED
            notif.error = error_msg or 'Unknown error'
        db.commit()

        if not success:
            # Можно ретраить
            raise Exception(f'Send failed: {error_msg}')

        return {'notification_id': notif.id, 'status': 'sent'}

    except Exception as e:
        db.rollback()
        logger.exception('Task failed')
        # Повторная попытка
        raise self.retry(exc=e)
    finally:
        db.close()


@shared_task
def broadcast_notification(template_id: int, context: dict = None, type_: str = None,
                           subject: str = None, body: str = None):
    user_ids = UserService.get_all_user_ids()
    for uid in user_ids:
        send_notification.delay(uid, template_id=template_id, context=context,
                                type_=type_, subject=subject, body=body)
    return {'users_count': len(user_ids)}
