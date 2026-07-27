import redis.asyncio as aioredis

from common.settings import get_settings

settings = get_settings()

redis_client = aioredis.from_url(settings.redis.url, decode_responses=True)