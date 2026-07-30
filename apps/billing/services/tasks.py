import asyncio
from datetime import datetime, timedelta, timezone

from celery import shared_task
from common import get_logger
from db.models import UserSubscription, UserSubscriptionPlan
from db.sync_database import SyncSessionLocal
from external_services.auth import update_user_premium_status
from external_services.notifier import send_notification

logger = get_logger(__name__)

# Вспомогательные асинхронные функции для вызовов
async def _update_and_notify(user_id: str, is_premium: bool, plan: str):
    await update_user_premium_status(user_id, is_premium)
    send_notification(user_id, 'subscription_activated', {'plan': plan})

async def _expire_and_notify(user_id: str):
    await update_user_premium_status(user_id, False)
    send_notification(user_id, 'subscription_expired', {})

@shared_task
def activate_subscription_task(user_id: str, plan: str, days: int | None = None, promo_code_id: str | None = None):
    db = SyncSessionLocal()
    try:
        sub = db.query(UserSubscription).filter(UserSubscription.user_id == user_id).first()
        if not sub:
            sub = UserSubscription(user_id=user_id, plan=UserSubscriptionPlan.FREE)
            db.add(sub)
        sub.plan = plan
        sub.start_date = datetime.now(timezone.utc)
        sub.end_date = (datetime.now(timezone.utc) + timedelta(days=days)) if days else None
        sub.is_active = True
        sub.promo_code_id = promo_code_id
        db.commit()

        is_premium = (plan == UserSubscriptionPlan.PREMIUM or plan == UserSubscriptionPlan.TRIAL)
        # Запускаем асинхронные вызовы
        asyncio.run(_update_and_notify(user_id, is_premium, plan))
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
            asyncio.run(_expire_and_notify(user_id))
    finally:
        db.close()