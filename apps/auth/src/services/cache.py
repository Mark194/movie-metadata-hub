from typing import Any

from redis.asyncio import Redis


class CacheService:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def get(self, key: str) -> str | None:
        return await self.redis.get(key)

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        await self.redis.set(key, value, ex=ttl)

    async def delete(self, key: str) -> None:
        await self.redis.delete(key)
