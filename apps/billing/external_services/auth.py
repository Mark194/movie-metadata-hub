import httpx
from common import get_logger

from external_services.clients import auth_client

logger = get_logger(__name__)


async def update_user_premium_status(user_id: str, is_premium: bool):
    try:
        # Используем await с асинхронным клиентом
        response = await auth_client.patch(
            f"/api/v1/internal/users/{user_id}/premium", json={"is_premium": is_premium}
        )
        response.raise_for_status()
        logger.info(f"User {user_id} premium status updated to {is_premium}")
    except httpx.HTTPStatusError as e:
        logger.error(
            f"Auth service error for user {user_id}: {e.response.status_code} - {e.response.text}"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating premium status for {user_id}: {e}")
        raise
