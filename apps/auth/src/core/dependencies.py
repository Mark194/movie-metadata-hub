from functools import lru_cache
from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from db.redis import get_redis
from db.postgres import get_postgres
from services.cache import CacheService
from services.role import RoleService


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
async def get_role_service(db: AsyncSession = Depends(get_postgres)) -> RoleService:
    return RoleService(db)