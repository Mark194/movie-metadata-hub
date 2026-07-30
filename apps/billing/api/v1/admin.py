from uuid import UUID

from db.database import get_db
from db.models import PromoCodeStatus
from fastapi import APIRouter, Depends, Query
from services.admin_promo_service import AdminPromoService
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import require_admin
from api.schemas import (
    ApplyPromoAdminRequest,
    GenerateCodesRequest,
    GenerateCodesResponse,
    PromoCodeCreate,
    PromoCodeResponse,
    PromoCodeUpdate,
)

router = APIRouter(prefix='/admin/promo', tags=['admin'])


@router.post('/create', response_model=PromoCodeResponse)
async def create_promo(
        data: PromoCodeCreate,
        admin: dict = Depends(require_admin),
        db: AsyncSession = Depends(get_db)
):
    return await AdminPromoService.create(data, admin['user_id'], db)


@router.get('/list', response_model=list[PromoCodeResponse])
async def list_promo(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        status: PromoCodeStatus = None,
        code_filter: str | None = None,
        admin: dict = Depends(require_admin),
        db: AsyncSession = Depends(get_db)
):
    return await AdminPromoService.list_all(db, skip, limit, status, code_filter)


@router.post('/generate', response_model=GenerateCodesResponse)
async def generate_codes(
        req: GenerateCodesRequest,
        admin: dict = Depends(require_admin),
        db: AsyncSession = Depends(get_db)
):
    return await AdminPromoService.generate_codes(req, admin['user_id'], db)


@router.post('/apply')
async def apply_promo_to_user(
        req: ApplyPromoAdminRequest,
        admin: dict = Depends(require_admin),
        db: AsyncSession = Depends(get_db)
):
    return await AdminPromoService.apply_for_user(req.promo_code, req.user_id, db)


@router.put("/{promo_id}")
async def update_promo(
        promo_id: UUID,
        data: PromoCodeUpdate,
        admin: dict = Depends(require_admin),
        db: AsyncSession = Depends(get_db)
):
    return await AdminPromoService.update(promo_id, data.dict(exclude_unset=True), db)
