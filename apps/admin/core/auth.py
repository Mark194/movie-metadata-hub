import logging
from functools import lru_cache
from http import HTTPStatus

import httpx
from common import get_settings
from django.core.cache import cache

logger = logging.getLogger(__name__)

config = get_settings()

LOGIN_FAILED = 'Login failed for user %s: status=%s, body=%s'
TIMEOUT = 'Timeout during login request to %s'
REQUEST_ERROR = 'Request error during login request to %s'

PERMISSION_ERROR = 'Failed to fetch permissions: status=%s, body=%s'
PERMISSION_TIMEOUT = 'Failed to fetch permissions to %s'
PERMISSION_REQUEST = 'Request error while fetching permissions: %s'

LOGOUT_FAILED = 'Logout failed for user %s: status=%s, body=%s'
LOGOUT_TIMEOUT = 'Timeout during logout request to %s'
LOGOUT_REQUEST = 'Request error during logout request to %s'

REFRESH_TOKEN = 'Refresh token failed for user %s: status=%s, body=%s'
REFRESH_TOKEN_TIMEOUT = 'Timeout during refresh token request to %s'
REFRESH_TOKEN_REQUEST = 'Request error during refresh token request to %s'


class AuthServiceClient:
    def __init__(self):
        self.base_url = config.auth.url
        self.timeout = config.auth.timeout
        self.cache_ttl = config.auth.cache_ttl

    def login(self, login: str, password: str):
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    f'{self.base_url}/api/v1/auth/login',
                    json={'login': login, 'password': password}
                )
                if response.status_code == HTTPStatus.OK:
                    return response.json()
                else:
                    logger.warning(
                        LOGIN_FAILED,
                        login, response.status_code, response.text
                    )
                    return None
        except httpx.TimeoutException:
            logger.error(TIMEOUT, self.base_url)
        except httpx.RequestError as e:
            logger.error(REQUEST_ERROR, str(e))
        return None

    def get_user_permissions(self, access_token: str):
        cache_key = f'user_perms_{access_token[:20]}'
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.get(
                    f'{self.base_url}/api/v1/user/permissions',
                    headers={'Authorization': f'Bearer {access_token}'}
                )
                if response.status_code == HTTPStatus.OK:
                    data = response.json()
                    cache.set(cache_key, data, self.cache_ttl)
                    return data
                else:
                    logger.warning(
                        PERMISSION_ERROR,
                        response.status_code, response.text
                    )
                    return None
        except httpx.TimeoutException:
            logger.error(PERMISSION_TIMEOUT, self.base_url)
        except httpx.RequestError as e:
            logger.error(PERMISSION_REQUEST, str(e))
        return None

    def logout(self, refresh_token: str, access_token: str):
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    f'{self.base_url}/api/v1/auth/logout',
                    json={'refresh_token': refresh_token},
                    headers={'Authorization': f'Bearer {access_token}'}
                )
                if response.status_code >= HTTPStatus.BAD_REQUEST:
                    logger.warning(
                        LOGOUT_FAILED,
                        response.status_code, response.text
                    )
        except httpx.TimeoutException:
            logger.error(LOGOUT_TIMEOUT, self.base_url)
        except httpx.RequestError as e:
            logger.error(LOGOUT_REQUEST, str(e))

    async def refresh_token(self, refresh_token: str):
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f'{self.base_url}/api/v1/auth/refresh',
                    json={'refresh_token': refresh_token}
                )
                if response.status_code == HTTPStatus.OK:
                    return response.json()
                else:
                    logger.warning(
                        REFRESH_TOKEN,
                        response.status_code, response.text
                    )
                    return None
        except httpx.TimeoutException:
            logger.error(REFRESH_TOKEN_TIMEOUT, self.base_url)
        except httpx.RequestError as e:
            logger.error(REFRESH_TOKEN_REQUEST, str(e))
        return None


# Синглтон
@lru_cache
def get_auth_client() -> AuthServiceClient:
    return AuthServiceClient()
