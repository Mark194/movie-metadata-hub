import logging

from django.core.cache import cache
from django.http import JsonResponse
from django.utils.deprecation import MiddlewareMixin

from core.auth import get_auth_client

logger = logging.getLogger(__name__)


class AuthMiddleware(MiddlewareMixin):
    """
    Проверяет access_token в заголовке Authorization.
    Если токен валиден, сохраняет пользователя в request.
    При недоступности Auth-сервиса использует кэшированные данные.
    """
    def process_request(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return

        token = auth_header.split(' ')[1]
        request.auth_token = token

        # Пытаемся получить пользователя из кэша
        cached_user = self.get_cached_user(token)
        if cached_user:
            request.auth_user = cached_user
            return

        # Запрос к Auth-сервису с таймаутом
        try:
            client = get_auth_client()
            # Эндпоинт /user/me возвращает информацию о пользователе
            import httpx
            response = httpx.get(
                f"{client.base_url}/api/v1/user/me",
                headers={"Authorization": f"Bearer {token}"},
                timeout=client.timeout
            )
            if response.status_code == 200:
                user_data = response.json()
                # Кэшируем данные пользователя
                self.cache_user(token, user_data)
                request.auth_user = user_data
            else:
                request.auth_user = None
        except (httpx.TimeoutException, httpx.ConnectError):
            logger.warning("Auth service unavailable, using fallback")
            # Fallback: разрешаем только чтение, но без прав на изменение
            request.auth_user = {"is_authenticated": False, "is_superuser": False}
            request.auth_fallback = True

    def get_cached_user(self, token):
        return cache.get(f"auth_user_{token[:20]}")

    def cache_user(self, token, user_data):
        cache.set(f"auth_user_{token[:20]}", user_data, timeout=300)


class RequirePermissionMiddleware:
    """
    Проверяет, имеет ли пользователь нужное разрешение (для доступа к админке).
    Использует кэшированные права при недоступности Auth.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Пример: доступ к админке требует права "admin.access"
        if request.path.startswith('/admin/'):
            if not hasattr(request, 'auth_user') or not request.auth_user.get('is_superuser'):
                # Проверяем права через Auth-сервис или кэш
                if not self.check_permission(request, "admin.access"):
                    return JsonResponse({"error": "Forbidden"}, status=403)
        return self.get_response(request)

    def check_permission(self, request, perm):
        token = getattr(request, 'auth_token', None)
        if not token:
            return False

        cache_key = f"user_perm_{perm}_{token[:20]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            client = get_auth_client()
            import httpx
            response = httpx.get(
                f"{client.base_url}/api/v1/user/has_permission",
                params={"permission": perm},
                headers={"Authorization": f"Bearer {token}"},
                timeout=client.timeout
            )
            has_perm = response.status_code == 200 and response.json().get("has_permission", False)
            cache.set(cache_key, has_perm, 60)  # на минуту
            return has_perm
        except (httpx.TimeoutException, httpx.ConnectError):
            return False