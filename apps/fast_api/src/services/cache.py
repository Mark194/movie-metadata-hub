from typing import Optional, List, TypeVar, Generic, Type
from redis.asyncio import Redis
from utils.serializers import orjson_dumps, deserialize_list

T = TypeVar('T')


class CacheService(Generic[T]):
    """Сервис для работы с кешем Redis"""

    def __init__(self, redis: Redis, default_expire: int = 300):
        self.redis = redis
        self.default_expire = default_expire

    async def get(self, key: str, model_class: Type[T]) -> Optional[T]:
        data = await self.redis.get(key)
        if not data:
            return None
        return model_class.parse_raw(data)

    async def get_list(self, key: str, model_class: Type[T]) -> Optional[List[T]]:
        data = await self.redis.get(key)
        if not data:
            return None
        return deserialize_list(data, model_class)

    async def set(self, key: str, value: T, expire: Optional[int] = None):
        await self.redis.set(
            key,
            value.json(),
            ex=expire or self.default_expire
        )

    async def set_list(self, key: str, values: List[T], expire: Optional[int] = None):
        data = orjson_dumps(values)
        await self.redis.set(
            key,
            data,
            ex=expire or self.default_expire
        )

    async def delete(self, key: str):
        await self.redis.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self.redis.exists(key))
