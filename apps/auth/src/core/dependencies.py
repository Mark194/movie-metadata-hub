from functools import lru_cache
from http import HTTPStatus

import jwt
from common import get_settings
from db.postgres import get_postgres
from db.redis import get_redis
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from models.user import User
from redis.asyncio import Redis
from services.auth import AuthService
from services.cache import CacheService
from services.oauth import OAuthService
from services.role import RoleService
from services.user import UserService
from sqlalchemy.ext.asyncio import AsyncSession

security = HTTPBearer()
settings = get_settings()

INVALID_TOKEN = 'Invalid token'
INVALID_TOKEN_PAYLOAD = 'Invalid token payload'
TOKEN_REVOKED = 'Token revoked'
USER_NOT_FOUND = 'User not found'
FORBIDDEN = 'Forbidden'


@lru_cache
def get_cache_service(
        redis: Redis = Depends(get_redis),
) -> CacheService:
    return CacheService(redis)


@lru_cache
def get_auth_service(
        cache_service: CacheService = Depends(get_cache_service),
        postgres: AsyncSession = Depends(get_postgres),
):
    return AuthService(cache_service, postgres)


@lru_cache
def get_role_service(db: AsyncSession = Depends(get_postgres)) -> RoleService:
    return RoleService(db)


async def get_user_service(
        db: AsyncSession = Depends(get_postgres),
        cache: CacheService = Depends(get_cache_service),
) -> UserService:
    return UserService(db, cache)


async def get_access_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    return credentials.credentials


async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: AsyncSession = Depends(get_postgres),
        cache: CacheService = Depends(get_cache_service),  # добавить зависимость
) -> User:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, settings.jwt.secret_key, algorithms=[settings.jwt.algorithm])
        user_id = payload.get("sub")
        jti = payload.get("jti")
        if not user_id or not jti:
            raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail=INVALID_TOKEN_PAYLOAD)

        if await cache.get(f'blacklist:{jti}'):
            raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail=TOKEN_REVOKED)

    except jwt.PyJWTError:
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail=INVALID_TOKEN)

    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail=USER_NOT_FOUND)
    return user


def require_permission(permission_name: str):
    async def permission_checker(
            current_user: User = Depends(get_current_user),
            db: AsyncSession = Depends(get_postgres),
    ):
        if current_user.is_superuser:
            return current_user

        for role in current_user.roles:
            for perm in role.permissions:
                if perm.name == permission_name:
                    return current_user
        raise HTTPException(status_code=HTTPStatus.FORBIDDEN, detail=FORBIDDEN)

    return permission_checker


async def get_oauth_service(
        db: AsyncSession = Depends(get_postgres),
        auth_service: AuthService = Depends(get_auth_service),
) -> OAuthService:
    return OAuthService(db, auth_service)
