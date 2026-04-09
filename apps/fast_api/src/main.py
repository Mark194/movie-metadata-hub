from contextlib import asynccontextmanager

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import films
from common import get_settings
from db import elastic, redis


config = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis.redis = Redis(host=config.redis.host, port=config.redis.port)
    elastic.es = AsyncElasticsearch(hosts=[config.elastic.url])
    yield
    await redis.redis.close()
    await elastic.es.close()


app = FastAPI(
    title=config.api.project_name,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.include_router(films.router, prefix='/api/v1', tags=['films'])
