import requests

from common.settings import get_settings

settings = get_settings()

async def update_user_premium_status(user_id: str, is_premium: bool):
    url = f'{settings.auth.url}/api/v1/internal/users/{user_id}/premium'
    headers = {'X-Internal-Token': settings.internal_api_token}
    data = {'is_premium': is_premium}
    try:
        resp = requests.patch(url, json=data, headers=headers, timeout=settings.auth.timeout)
        resp.raise_for_status()
    except Exception as e:
        # Логируем ошибку
        pass