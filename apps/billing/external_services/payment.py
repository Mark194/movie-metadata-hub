import httpx
from common import logger

from external_services.clients import payment_client

logger = get_logger(__name__)


async def process_payment(order_id: str, amount: float, payment_method: str):
    try:
        response = await payment_client.post(
            "/pay",
            json={
                "order_id": order_id,
                "amount": amount,
                "payment_method": payment_method,
            },
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(
            f"Payment gateway error: {e.response.status_code} - {e.response.text}"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error during payment: {e}")
        raise
