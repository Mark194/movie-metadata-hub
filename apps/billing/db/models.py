from enum import StrEnum
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.sql import func

from db.database import Base


class DiscountType(StrEnum):
    PERCENT = "percent"
    FIXED = "fixed"
    FREE_TRIAL = "free_trial"


class PromoCodeStatus(StrEnum):
    ACTIVE = "active"
    EXPIRED = "expired"
    DISABLED = "disabled"


class UsageStatus(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REVERTED = "reverted"


class PromoCode(Base):
    __tablename__ = "promo_codes"

    id = Column(PGUUID, primary_key=True, default=uuid4)
    code = Column(String(50), unique=True, index=True, nullable=False)
    discount_type = Column(Enum(DiscountType), nullable=False)
    discount_value = Column(Integer, nullable=False)  # процент/сумма/кол-во дней
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=True)  # NULL = бессрочный
    max_uses = Column(Integer, default=0)  # 0 = неограничено
    used_count = Column(Integer, default=0)
    is_single_use = Column(Boolean, default=True)  # True = одноразовый на пользователя
    created_by = Column(PGUUID, ForeignKey("users.id"), nullable=False)
    status = Column(Enum(PromoCodeStatus), default=PromoCodeStatus.ACTIVE)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class PromoCodeUsage(Base):
    __tablename__ = "promo_code_usages"

    id = Column(PGUUID, primary_key=True, default=uuid4)
    promo_code_id = Column(PGUUID, ForeignKey("promo_codes.id"), nullable=False)
    user_id = Column(PGUUID, ForeignKey("users.id"), nullable=False)
    applied_at = Column(DateTime(timezone=True), default=func.now())
    order_id = Column(PGUUID, nullable=True)  # связь с заказом (если есть)
    status = Column(Enum(UsageStatus), default=UsageStatus.PENDING)


class UserSubscriptionPlan(StrEnum):
    FREE = "free"
    TRIAL = "trial"
    PREMIUM = "premium"


class UserSubscription(Base):
    __tablename__ = "user_subscriptions"

    id = Column(PGUUID, primary_key=True, default=uuid4)
    user_id = Column(PGUUID, ForeignKey("users.id"), unique=True, nullable=False)
    plan = Column(String(20), default=UserSubscriptionPlan.FREE)
    start_date = Column(DateTime(timezone=True), nullable=False, default=func.now())
    end_date = Column(DateTime(timezone=True), nullable=True)  # NULL для бессрочной
    is_active = Column(Boolean, default=True)
    promo_code_id = Column(PGUUID, ForeignKey("promo_codes.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
