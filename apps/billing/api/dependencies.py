from http import HTTPStatus

from common.settings import get_settings
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from services.redis_client import redis_client

settings = get_settings()
security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm]
        )
        user_id = payload.get("sub")
        jti = payload.get("jti")
        if not user_id or not jti:
            raise HTTPException(HTTPStatus.UNAUTHORIZED, "Invalid token payload")
        # Проверка чёрного списка
        if await redis_client.get(f"blacklist:{jti}"):
            raise HTTPException(HTTPStatus.UNAUTHORIZED, "Token revoked")
        role = payload.get("role", "user")
        return {"user_id": user_id, "role": role}
    except JWTError:
        raise HTTPException(HTTPStatus.UNAUTHORIZED, "Invalid token")


async def require_admin(current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin rights required")
    return current_user
