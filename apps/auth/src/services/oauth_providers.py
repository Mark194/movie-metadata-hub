from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import urlencode

import httpx
from common.settings import get_settings

settings = get_settings()


class BaseOAuthProvider(ABC):
    def __init__(self, provider_name: str):
        self.provider_name = provider_name
        self.config = settings.oauth.dict()[provider_name]

    @abstractmethod
    def get_auth_url(self) -> str:
        """Возвращает URL для редиректа на страницу авторизации провайдера."""

    @abstractmethod
    async def get_access_token(self, code: str) -> dict[str, Any]:
        """Обменивает code на access_token и возвращает полный ответ провайдера."""

    @abstractmethod
    async def get_user_info(self, token_data: dict[str, Any]) -> dict[str, Any]:
        """Извлекает данные пользователя из ответа провайдера и приводит к единому формату."""


class YandexOAuthProvider(BaseOAuthProvider):
    def __init__(self):
        super().__init__('yandex')
        self.auth_url = 'https://oauth.yandex.ru/authorize'
        self.token_url = 'https://oauth.yandex.ru/token'
        self.userinfo_url = 'https://login.yandex.ru/info'
        self.scope = 'login:email login:info'

    def get_auth_url(self) -> str:
        params = {
            'response_type': 'code',
            'client_id': self.config['client_id'],
            'redirect_uri': self.config['redirect_uri'],
            'scope': self.scope,
        }
        return f'{self.auth_url}?{urlencode(params)}'

    async def get_access_token(self, code: str) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            data = {
                'grant_type': 'authorization_code',
                'code': code,
                'client_id': self.config['client_id'],
                'client_secret': self.config['client_secret'],
            }
            response = await client.post(self.token_url, data=data)
            response.raise_for_status()
            return response.json()

    async def get_user_info(self, token_data: dict[str, Any]) -> dict[str, Any]:
        access_token = token_data['access_token']
        async with httpx.AsyncClient() as client:
            headers = {'Authorization': f'OAuth {access_token}'}
            response = await client.get(self.userinfo_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return {
                'provider_user_id': str(data['id']),
                'email': data.get('default_email', data.get('login')),
                'first_name': data.get('first_name', ''),
                'last_name': data.get('last_name', ''),
            }


class VKOAuthProvider(BaseOAuthProvider):
    def __init__(self):
        super().__init__('vk')
        self.auth_url = 'https://oauth.vk.com/authorize'
        self.token_url = 'https://oauth.vk.com/access_token'
        self.userinfo_url = 'https://api.vk.com/method/users.get'
        self.scope = 'email'
        self.api_version = '5.131'

    def get_auth_url(self) -> str:
        params = {
            'response_type': 'code',
            'client_id': self.config['client_id'],
            'redirect_uri': self.config['redirect_uri'],
            'scope': self.scope,
            'v': self.api_version,
        }
        return f'{self.auth_url}?{urlencode(params)}'

    async def get_access_token(self, code: str) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            params = {
                'client_id': self.config['client_id'],
                'client_secret': self.config['client_secret'],
                'redirect_uri': self.config['redirect_uri'],
                'code': code,
            }
            response = await client.get(self.token_url, params=params)
            response.raise_for_status()
            return response.json()

    async def get_user_info(self, token_data: dict[str, Any]) -> dict[str, Any]:
        access_token = token_data['access_token']
        user_id = token_data['user_id']
        async with httpx.AsyncClient() as client:
            params = {
                'user_ids': user_id,
                'fields': 'first_name,last_name,email',
                'access_token': access_token,
                'v': self.api_version,
            }
            response = await client.get(self.userinfo_url, params=params)
            response.raise_for_status()
            data = response.json()['response'][0]
            return {
                'provider_user_id': str(data['id']),
                'email': data.get('email', f'vk_{data['id']}@vk.com'),
                'first_name': data['first_name'],
                'last_name': data['last_name'],
            }


def get_provider(provider_name: str) -> BaseOAuthProvider:
    """Фабрика провайдеров."""
    providers = {
        'yandex': YandexOAuthProvider,
        'vk': VKOAuthProvider,
    }
    if provider_name not in providers:
        raise ValueError(f'Unsupported provider: {provider_name}')
    return providers[provider_name]()