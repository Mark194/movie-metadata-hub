from datetime import datetime
from uuid import UUID

from db.models import DiscountType, PromoCodeStatus
from pydantic import BaseModel, Field


class PromoCodeCreate(BaseModel):
    code: str
    discount_type: DiscountType
    discount_value: int
    valid_from: datetime
    valid_until: datetime | None = None
    max_uses: int = 0
    is_single_use: bool = True


class PromoCodeUpdate(BaseModel):
    status: PromoCodeStatus = None
    valid_until: datetime | None = None
    max_uses: int


class PromoCodeResponse(BaseModel):
    id: UUID
    code: str
    discount_type: DiscountType
    discount_value: int
    valid_from: datetime
    valid_until: datetime | None
    max_uses: int
    used_count: int
    is_single_use: bool
    status: PromoCodeStatus
    created_at: datetime


class ApplyPromoRequest(BaseModel):
    promo_code: str


class ApplyPromoResponse(BaseModel):
    usage_id: UUID
    discount_type: DiscountType
    discount_value: int
    message: str


class ConfirmPaymentRequest(BaseModel):
    usage_id: UUID
    order_id: UUID
    payment_success: bool


class GenerateCodesRequest(BaseModel):
    count: int = Field(..., ge=1, le=1000)
    length: int = Field(8, ge=4, le=20)
    prefix: str = Field('', max_length=10)
    discount_type: DiscountType
    discount_value: int = Field(..., gt=0)
    valid_from: datetime
    valid_until: datetime | None = None
    max_uses: int = Field(0, ge=0)
    is_single_use: bool = True


class GenerateCodesResponse(BaseModel):
    generated_codes: list[str]
    count: int


class ApplyPromoAdminRequest(BaseModel):
    promo_code: str
    user_id: UUID


class SubscriptionResponse(BaseModel):
    plan: str
    is_active: bool
    end_date: datetime | None = None
