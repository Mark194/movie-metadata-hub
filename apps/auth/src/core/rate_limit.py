from common.settings import get_settings
from redis import asyncio as aioredis
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

settings = get_settings()

STORAGE_OPTIONS = {'decode_responses': True}


async def get_redis_client():
    return aioredis.from_url(settings.redis.url, decode_responses=True)


limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=settings.redis.url,
    storage_options=STORAGE_OPTIONS,
)


def setup_rate_limiting(app):
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)
