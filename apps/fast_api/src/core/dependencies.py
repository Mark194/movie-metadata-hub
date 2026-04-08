from functools import lru_cache

from fastapi import Depends
from redis.asyncio import Redis
from elasticsearch import AsyncElasticsearch

from db.redis import get_redis
from db.elastic import get_elastic
from services.film import FilmService
from services.cache import CacheService


@lru_cache()
def get_cache_service(
        redis: Redis = Depends(get_redis),
) -> CacheService:
    return CacheService(redis)


@lru_cache()
def get_film_service(
        cache_service: CacheService = Depends(get_cache_service),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(cache_service, elastic)
