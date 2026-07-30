from redis.asyncio import Redis

redis: Redis | None = None


async def get_redis() -> Redis:
    if redis is None:
        raise RuntimeError("Redis not initialized")
    return redis


async def close_redis():
    global redis
    if redis:
        await redis.close()
        redis = None
