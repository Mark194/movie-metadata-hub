from datetime import datetime, timezone
from http import HTTPStatus
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.settings import get_settings
from db.models import PromoCode, PromoCodeUsage, UsageStatus, DiscountType, PromoCodeStatus
from external_services.auth import update_user_premium_status
from external_services.notifier import send_notification_async
from services.celery_app import celery_app
from services.tasks import activate_subscription_task

settings = get_settings()

PROMO_NOT_FOUND = 'Промокод не найден'
PROMO_INACTIVE = 'Промокод неактивен'
PROMO_NOT_ACTIVE = 'Промокод ещё не начал действовать'
PROMO_EXPIRED = 'Промокод истёк'
PROMO_USED = 'Вы уже использовали этот промокод'
LIMIT_REACHED = 'Лимит использований исчерпан'
PROMO_APPLIED = 'Промокод применён, ожидает оплаты'

USAGE_NOT_FOUND = 'Usage не найден'
USAGE_ALREADY_HANDLE = 'Usage уже обработан'
USAGE_NOT_WAITING = 'Usage не в статусе ожидания'

STATUS_CONFIRMED = {'status': 'confirmed'}
STATUS_REVERTED = {'status': 'reverted'}


class PromoService:
    @staticmethod
    async def validate_and_apply(promo_code: str, user_id: UUID, db: AsyncSession):
        # 1. Найти промокод
        stmt = select(PromoCode).where(PromoCode.code == promo_code)
        result = await db.execute(stmt)
        promo = result.scalar_one_or_none()
        if not promo:
            raise HTTPException(HTTPStatus.NOT_FOUND, PROMO_NOT_FOUND)

        # 2. Проверить статус
        if promo.status != PromoCodeStatus.ACTIVE:
            raise HTTPException(HTTPStatus.BAD_REQUEST, PROMO_INACTIVE)

        # 3. Проверить даты
        now = datetime.now(timezone.utc)
        if promo.valid_from > now:
            raise HTTPException(HTTPStatus.BAD_REQUEST, PROMO_NOT_ACTIVE)
        if promo.valid_until and promo.valid_until < now:
            raise HTTPException(HTTPStatus.BAD_REQUEST, PROMO_EXPIRED)

        # 4. Проверить лимит использований
        if 0 < promo.max_uses <= promo.used_count:
            raise HTTPException(HTTPStatus.BAD_REQUEST, LIMIT_REACHED)

        # 5. Если одноразовый, проверить, не использовал ли уже этот пользователь
        if promo.is_single_use:
            stmt = select(PromoCodeUsage).where(
                PromoCodeUsage.promo_code_id == promo.id,
                PromoCodeUsage.user_id == user_id,
                PromoCodeUsage.status.in_([UsageStatus.PENDING, UsageStatus.CONFIRMED])
            )
            result = await db.execute(stmt)
            if result.scalar_one_or_none():
                raise HTTPException(HTTPStatus.BAD_REQUEST, PROMO_USED)

        # 6. Создать usage со статусом PENDING
        usage = PromoCodeUsage(
            promo_code_id=promo.id,
            user_id=user_id,
            status=UsageStatus.PENDING
        )
        db.add(usage)
        await db.commit()
        await db.refresh(usage)

        # 7. Если промокод FREE_TRIAL – можно сразу активировать подписку (без оплаты)
        # Но по условию требуется подтверждение оплаты, поэтому оставляем PENDING
        # Возвращаем данные для оплаты
        return {
            'usage_id': usage.id,
            'discount_type': promo.discount_type,
            'discount_value': promo.discount_value,
            'message': PROMO_APPLIED
        }

    @staticmethod
    async def confirm_usage(usage_id: UUID, order_id: UUID, db: AsyncSession):
        # Найти usage
        stmt = select(PromoCodeUsage).where(PromoCodeUsage.id == usage_id)
        result = await db.execute(stmt)
        usage = result.scalar_one_or_none()
        if not usage:
            raise HTTPException(HTTPStatus.NOT_FOUND, USAGE_NOT_FOUND)

        if usage.status != UsageStatus.PENDING:
            raise HTTPException(HTTPStatus.BAD_REQUEST, USAGE_ALREADY_HANDLE)

        # Блокируем строку промокода
        promo_stmt = select(PromoCode).where(PromoCode.id == usage.promo_code_id).with_for_update()
        promo_result = await db.execute(promo_stmt)
        promo = promo_result.scalar_one()

        # Атомарно проверяем лимит и обновляем
        if 0 < promo.max_uses <= promo.used_count:
            raise HTTPException(HTTPStatus.BAD_REQUEST, LIMIT_REACHED)

        # Обновляем usage и инкремент в одной транзакции
        usage.status = UsageStatus.CONFIRMED
        usage.order_id = order_id
        promo.used_count += 1

        await db.commit()  # Единый коммит

        # В зависимости от типа промокода активируем подписку
        if promo.discount_type == DiscountType.FREE_TRIAL:
            # Активировать триал
            await PromoService._activate_subscription(
                user_id=usage.user_id,
                plan='trial',
                days=promo.discount_value,
                promo_code_id=promo.id
            )
        else:
            # Для percent/fixed – предполагается, что оплата уже прошла,
            # и подписка активируется отдельно (например, через payment service)
            # Здесь можно активировать premium подписку
            await PromoService._activate_subscription(
                user_id=usage.user_id,
                plan='premium',
                days=None,  # бессрочная или на период
                promo_code_id=promo.id
            )

        # Отправить уведомление
        await send_notification_async(
            user_id=usage.user_id,
            template_name='promo_applied',
            context={'promo_code': promo.code}
        )

        return STATUS_CONFIRMED

    @staticmethod
    async def revert_usage(usage_id: UUID, db: AsyncSession):
        # Отмена оплаты – меняем статус на REVERTED
        stmt = select(PromoCodeUsage).where(PromoCodeUsage.id == usage_id)
        result = await db.execute(stmt)
        usage = result.scalar_one_or_none()
        if not usage:
            raise HTTPException(HTTPStatus.NOT_FOUND, USAGE_NOT_FOUND)

        if usage.status != UsageStatus.PENDING:
            raise HTTPException(HTTPStatus.BAD_REQUEST, USAGE_NOT_WAITING)

        usage.status = UsageStatus.REVERTED
        await db.commit()

        # Если подписка была активирована (для триала) – деактивировать
        # (но в нашем случае мы активируем только при подтверждении, так что здесь не нужно)
        return STATUS_REVERTED

    @staticmethod
    async def _activate_subscription(user_id: UUID, plan: str, days: int | None = None,
                                     promo_code_id: UUID | None = None):
        activate_subscription_task.delay(str(user_id), plan, days, str(promo_code_id) if promo_code_id else None)
