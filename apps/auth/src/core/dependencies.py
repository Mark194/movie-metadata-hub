from functools import lru_cache
from http import HTTPStatus

import jwt
from common import get_settings
from db.postgres import get_postgres
from db.redis import get_redis
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from services.cache import CacheService
from services.role import RoleService
from sqlalchemy.ext.asyncio import AsyncSession

from models.user import User

security = HTTPBearer()
settings = get_settings()


INVALID_TOKEN = 'Invalid token'
USER_NOT_FOUND = 'User not found'
FORBIDDEN = 'Forbidden'


@lru_cache()
def get_cache_service(
        redis: Redis = Depends(get_redis),
) -> CacheService:
    return CacheService(redis)


@lru_cache()
def get_auth_service(
        cache_service: CacheService = Depends(get_cache_service),
        postgres: AsyncSession = Depends(get_postgres),
):
    from services.auth import AuthService
    return AuthService(cache_service, postgres)


@lru_cache()
def get_role_service(db: AsyncSession = Depends(get_postgres)) -> RoleService:
    return RoleService(db)


async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: AsyncSession = Depends(get_postgres),
) -> User:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, settings.jwt.secret_key, algorithms=[settings.jwt.algorithm])
        user_id = payload.get('sub')
        if not user_id:
            raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail=INVALID_TOKEN)
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