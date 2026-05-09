from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from db.postgres import init_postgres, close_postgres
from db.redis import close_redis
from common.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    global global_redis
    global_redis = Redis.from_url(
        settings.redis.url,
        decode_responses=True
    )

    await init_postgres(settings.postgres.db_url)

    yield

    await close_redis()
    await close_postgres()


app = FastAPI(lifespan=lifespan)
