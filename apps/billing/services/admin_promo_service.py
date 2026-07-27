import secrets
import string

from uuid import UUID
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException, status

from api.schemas import PromoCodeCreate, GenerateCodesRequest, ApplyPromoAdminRequest
from db.models import PromoCode, PromoCodeStatus, UserSubscriptionPlan, DiscountType, PromoCodeUsage, UsageStatus
from services.promo_service import PromoService

PROMO_NOT_FOUND = 'Промокод не найден'
PROMO_ALREADY_EXIST = 'Промокод с таким кодом уже существует'
INCORRECT_END_DATE = 'Дата окончания должна быть позже даты начала'
PROMO_NOT_FOUND_OR_INACTIVE = 'Промокод не найден или неактивен'
PROMO_LIMITED = 'Лимит использований исчерпан'
PROMO_ALREADY_USED = 'Пользователь уже использовал этот промокод'

COUNT_MUST_PE_POSITIVE = 'Количество должно быть положительным'
PROMO_MINIMUM_LENGTH = 'Минимальная длина кода 4 символа'


class AdminPromoService:
    @staticmethod
    async def create(data: PromoCodeCreate, admin_user_id: UUID, db: AsyncSession):
        # Проверка уникальности кода
        stmt = select(PromoCode).where(PromoCode.code == data.code)
        result = await db.execute(stmt)
        if result.scalar_one_or_none():
            raise HTTPException(status.HTTP_400_BAD_REQUEST, PROMO_ALREADY_EXIST)

        # Проверка корректности дат
        if data.valid_until and data.valid_until <= data.valid_from:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, INCORRECT_END_DATE)

        promo = PromoCode(
            code=data.code,
            discount_type=data.discount_type,
            discount_value=data.discount_value,
            valid_from=data.valid_from,
            valid_until=data.valid_until,
            max_uses=data.max_uses,
            is_single_use=data.is_single_use,
            created_by=admin_user_id,
            status=PromoCodeStatus.ACTIVE,
        )
        db.add(promo)
        await db.commit()
        await db.refresh(promo)
        return promo

    @staticmethod
    async def list_all(
            db: AsyncSession,
            skip: int = 0,
            limit: int = 100,
            status: PromoCodeStatus = None,
            code_filter: str | None = None,
    ):
        stmt = select(PromoCode)
        if status:
            stmt = stmt.where(PromoCode.status == status)
        if code_filter:
            stmt = stmt.where(PromoCode.code.ilike(f'%{code_filter}%'))
        stmt = stmt.offset(skip).limit(limit).order_by(PromoCode.created_at.desc())
        result = await db.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def update(promo_id: UUID, data: dict, db: AsyncSession):
        stmt = select(PromoCode).where(PromoCode.id == promo_id)
        result = await db.execute(stmt)
        promo = result.scalar_one_or_none()
        if not promo:
            raise HTTPException(status.HTTP_404_NOT_FOUND, PROMO_NOT_FOUND)
        # Обновляем только разрешённые поля
        for field in ('status', 'valid_until', 'max_uses'):
            if field in data:
                setattr(promo, field, data[field])
        await db.commit()
        await db.refresh(promo)
        return promo

    @staticmethod
    async def generate_codes(
            req: GenerateCodesRequest,
            admin_user_id: UUID,
            db: AsyncSession
    ):
        if req.count <= 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, COUNT_MUST_PE_POSITIVE)
        if req.length < 4:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, PROMO_MINIMUM_LENGTH)
        if req.valid_until and req.valid_until <= req.valid_from:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, INCORRECT_END_DATE)

        # Получаем все существующие коды для избежания дубликатов
        stmt = select(PromoCode.code)
        result = await db.execute(stmt)
        existing_codes = set(result.scalars().all())

        generated = []
        attempts = 0
        max_attempts = req.count * 10  # защита от бесконечного цикла

        while len(generated) < req.count and attempts < max_attempts:
            attempts += 1
            # Генерация случайной части
            random_part = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(req.length))
            code = f'{req.prefix}_{random_part}' if req.prefix else random_part
            if code in existing_codes:
                continue
            existing_codes.add(code)
            generated.append(code)

            # Создаём запись промокода
            promo = PromoCode(
                code=code,
                discount_type=req.discount_type,
                discount_value=req.discount_value,
                valid_from=req.valid_from,
                valid_until=req.valid_until,
                max_uses=req.max_uses,
                is_single_use=req.is_single_use,
                created_by=admin_user_id,
                status=PromoCodeStatus.ACTIVE,
            )
            db.add(promo)

        await db.commit()
        return {'generated_codes': generated, 'count': len(generated)}

    @staticmethod
    async def apply_for_user(promo_code: str, user_id: UUID, db: AsyncSession):
        # Ищем промокод
        stmt = select(PromoCode).where(PromoCode.code == promo_code, PromoCode.status == PromoCodeStatus.ACTIVE)
        result = await db.execute(stmt)
        promo = result.scalar_one_or_none()
        if not promo:
            raise HTTPException(status.HTTP_404_NOT_FOUND, PROMO_NOT_FOUND_OR_INACTIVE)

        # Проверка лимита
        if 0 < promo.max_uses <= promo.used_count:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, PROMO_LIMITED)

        # Проверка, что пользователь уже не использовал этот промокод (если одноразовый)
        if promo.is_single_use:
            stmt = select(PromoCodeUsage).where(
                PromoCodeUsage.promo_code_id == promo.id,
                PromoCodeUsage.user_id == user_id,
                PromoCodeUsage.status.in_([UsageStatus.PENDING, UsageStatus.CONFIRMED])
            )
            result = await db.execute(stmt)
            if result.scalar_one_or_none():
                raise HTTPException(status.HTTP_400_BAD_REQUEST, PROMO_ALREADY_USED)

        # Создаём usage со статусом CONFIRMED
        usage = PromoCodeUsage(
            promo_code_id=promo.id,
            user_id=user_id,
            status=UsageStatus.CONFIRMED,
            applied_at=datetime.utcnow()
        )
        db.add(usage)
        promo.used_count += 1
        await db.commit()

        # Активируем подписку (в зависимости от типа промокода)
        if promo.discount_type == DiscountType.FREE_TRIAL:
            await PromoService._activate_subscription(
                user_id=user_id,
                plan=UserSubscriptionPlan.TRIAL,
                days=promo.discount_value,
                promo_code_id=promo.id
            )
        else:
            # Для процентной или фиксированной скидки – активируем полную подписку (премиум)
            await PromoService._activate_subscription(
                user_id=user_id,
                plan=UserSubscriptionPlan.PREMIUM,
                days=None,
                promo_code_id=promo.id
            )

        return {'status': 'applied', 'usage_id': usage.id}
