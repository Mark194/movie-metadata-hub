from functools import lru_cache
from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from db.redis import get_redis
from db.postgres import get_postgres
from services.cache import CacheService

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
    from services.auth import AuthService  # локальный импорт
    return AuthService(cache_service, postgres)