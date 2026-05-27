import httpx
import logging

from django.core.cache import cache
from http import HTTPStatus
from functools import lru_cache

from common import get_settings

logger = logging.getLogger(__name__)

config = get_settings()


class AuthServiceClient:
    def __init__(self):
        self.base_url = config.auth.url
        self.timeout = config.auth.timeout
        self.cache_ttl = config.auth.cache_ttl

    def login(self, login: str, password: str):
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                f"{self.base_url}/api/v1/auth/login",
                json={"login": login, "password": password}
            )
            if response.status_code == HTTPStatus.OK:
                return response.json()
            return None

    def get_user_permissions(self, access_token: str):
        cache_key = f"user_perms_{access_token[:20]}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(
                f"{self.base_url}/api/v1/user/permissions",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            if response.status_code == HTTPStatus.OK:
                data = response.json()
                cache.set(cache_key, data, self.cache_ttl)
                return data
        return None

    def logout(self, refresh_token: str, access_token: str):
        """Выход (инвалидация токенов)."""
        with httpx.Client(timeout=self.timeout) as client:
            client.post(
                f"{self.base_url}/api/v1/auth/logout",
                json={"refresh_token": refresh_token},
                headers={"Authorization": f"Bearer {access_token}"}
            )

    async def refresh_token(self, refresh_token: str):
        """Обновление токенов."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/v1/auth/refresh",
                json={"refresh_token": refresh_token}
            )
            if response.status_code == HTTPStatus.OK:
                return response.json()
        return None


# Синглтон
@lru_cache
def get_auth_client() -> AuthServiceClient:
    return AuthServiceClient()
