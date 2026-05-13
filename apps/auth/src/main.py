from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from api.v1.auth import router as auth_router
from api.v1.roles import router as roles_router
from api.v1.users import router as users_router
from db.postgres import init_postgres, close_postgres, Base
from db import redis
from db.redis import close_redis
from common.settings import get_settings

APP_PREFIX = '/api/v1'

config = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    redis.redis = Redis(host=config.redis.host, port=config.redis.port)

    await init_postgres(settings.postgres.async_db_url)

    yield

    await close_redis()
    await close_postgres()


app = FastAPI(
    lifespan=lifespan,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
)
app.include_router(auth_router, prefix=APP_PREFIX)
app.include_router(roles_router, prefix=APP_PREFIX)
app.include_router(users_router, prefix=APP_PREFIX)
