from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from api.v1.auth import router as auth_router
from api.v1.roles import router as roles_router
from api.v1.users import router as users_router
from db.postgres import init_postgres, close_postgres
from db.redis import close_redis
from common.settings import get_settings

APP_PREFIX = '/api/v1'

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
app.include_router(auth_router, prefix=APP_PREFIX)
app.include_router(roles_router, prefix=APP_PREFIX)
app.include_router(users_router, prefix=APP_PREFIX)
