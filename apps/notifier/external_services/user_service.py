from collections.abc import Iterator
from typing import Any
from uuid import UUID

import requests
from common import get_logger, get_settings

settings = get_settings()
logger = get_logger(__name__)


class UserService:
    @staticmethod
    def get_user(user_id: UUID):
        try:
            url = f"{settings.auth.url}/api/v1/internal/users/{user_id}"
            headers = {"X-Internal-Token": settings.auth.internal_token}
            response = requests.get(url, timeout=settings.auth.timeout, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch user {user_id}: {e}")
            raise RuntimeError(f"User service unavailable: {e}")

    @staticmethod
    def iter_users(page_size: int = 100) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            url = f"{settings.auth.url}/api/v1/internal/users"
            params = {"limit": page_size, "offset": offset}
            headers = {"X-Internal-Token": settings.auth.internal_token}
            resp = requests.get(
                url, params=params, headers=headers, timeout=settings.auth.timeout
            )
            resp.raise_for_status()
            data = resp.json()
            users = data.get("users", [])
            if not users:
                break
            yield from users
            if len(users) < page_size:
                break
            offset += page_size

    @staticmethod
    def get_all_user_ids():
        try:
            url = f"{settings.auth.url}/api/v1/internal/users"
            headers = {"X-Internal-Token": settings.auth.internal_token}
            response = requests.get(url, timeout=settings.auth.timeout, headers=headers)
            response.raise_for_status()
            data = response.json()
            return [user["id"] for user in data]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch all users: {e}")
            raise RuntimeError(f"User service unavailable: {e}")
