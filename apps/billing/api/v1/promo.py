from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_current_user
from api.schemas import ApplyPromoRequest, ApplyPromoResponse, ConfirmPaymentRequest
from db.database import get_db
from services.promo_service import PromoService

router = APIRouter(prefix='/promo', tags=['promo'])


@router.post('/apply')
async def apply_promo(
        req: ApplyPromoRequest,
        current_user: dict = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
):
    result = await PromoService.validate_and_apply(req.promo_code, current_user['user_id'], db)
    return result


@router.post('/confirm-payment')
async def confirm_payment(
        req: ConfirmPaymentRequest,
        current_user: dict = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
):
    if req.payment_success:
        return await PromoService.confirm_usage(req.usage_id, req.order_id, db)
    else:
        return await PromoService.revert_usage(req.usage_id, db)
