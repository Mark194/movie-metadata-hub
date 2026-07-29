from celery import shared_task
from datetime import datetime, timedelta

from common import get_logger
from db.sync_database import SyncSessionLocal
from db.models import UserSubscription, UserSubscriptionPlan
from external_services.auth import update_user_premium_status
from external_services.notifier import send_notification

logger = get_logger(__name__)


@shared_task
def activate_subscription_task(user_id: str, plan: str, days: int = None, promo_code_id: str = None):
    db = SyncSessionLocal()
    try:
        # Найти или создать подписку
        sub = db.query(UserSubscription).filter(UserSubscription.user_id == user_id).first()
        if not sub:
            sub = UserSubscription(user_id=user_id, plan=UserSubscriptionPlan.FREE)
            db.add(sub)
        # Обновить
        sub.plan = plan
        sub.start_date = datetime.utcnow()
        if days:
            sub.end_date = datetime.utcnow() + timedelta(days=days)
        else:
            sub.end_date = None  # бессрочная
        sub.is_active = True
        sub.promo_code_id = promo_code_id
        db.commit()

        # Обновить статус в auth
        is_premium = (plan == UserSubscriptionPlan.PREMIUM or plan == UserSubscriptionPlan.TRIAL)
        update_user_premium_status(user_id, is_premium)

        # Отправить уведомление
        send_notification(user_id, 'subscription_activated', {'plan': plan})
    except Exception as e:
        logger.error(f'Failed to activate subscription for {user_id}: {e}')
        db.rollback()
        raise
    finally:
        db.close()


@shared_task
def expire_subscription_task(user_id: str):
    db = SyncSessionLocal()
    try:
        sub = db.query(UserSubscription).filter(UserSubscription.user_id == user_id).first()
        if sub:
            sub.is_active = False
            db.commit()
            update_user_premium_status(user_id, False)
            send_notification(user_id, 'subscription_expired', {})
    finally:
        db.close()
