from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_current_user
from api.schemas import SubscriptionResponse
from db.database import get_db
from db.models import UserSubscription, UserSubscriptionPlan

router = APIRouter(prefix='/subscription', tags=['subscription'])

FREE_PLAN = {'plan': UserSubscriptionPlan.FREE, 'is_active': False}


@router.get('/me', response_model=SubscriptionResponse)
async def get_my_subscription(
        current_user: dict = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
):
    stmt = select(UserSubscription).where(UserSubscription.user_id == current_user['user_id'])
    result = await db.execute(stmt)
    sub = result.scalar_one_or_none()
    if not sub:
        return FREE_PLAN
    return {'plan': sub.plan, 'is_active': sub.is_active, 'end_date': sub.end_date}
