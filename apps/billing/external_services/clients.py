import httpx

from common.settings import get_settings

settings = get_settings()

# Глобальный клиент для сервиса auth
auth_client = httpx.AsyncClient(
    base_url=settings.auth.url,  # например, "http://auth:8000"
    timeout=settings.auth.timeout,
    headers={"X-Internal-Token": settings.auth.internal_api_token}
)

# Глобальный клиент для платёжного шлюза
payment_client = httpx.AsyncClient(
    base_url=settings.billing.payment_gateway_url,
    timeout=settings.billing.payment_timeout
)