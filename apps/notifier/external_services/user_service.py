import requests

from common import get_settings, get_logger

settings = get_settings()
logger = get_logger(__name__)

class UserService:
    @staticmethod
    def get_user(user_id: int):
        try:
            url = f'{settings.auth.url}/api/v1/users/{user_id}'
            response = requests.get(url, timeout=settings.auth.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f'Failed to fetch user {user_id}: {e}')
            raise RuntimeError(f'User service unavailable: {e}')

    @staticmethod
    def get_all_user_ids():
        try:
            url = f'{settings.auth.url}/api/v1/users'
            response = requests.get(url, timeout=settings.auth.timeout)
            response.raise_for_status()
            data = response.json()
            return [user['id'] for user in data]
        except requests.exceptions.RequestException as e:
            logger.error(f'Failed to fetch all users: {e}')
            raise RuntimeError(f'User service unavailable: {e}')