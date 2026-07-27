from fastapi import FastAPI
from api.v1 import promo, subscription, admin
from db.database import engine, Base
from contextlib import asynccontextmanager
import logging

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


ROUTER_PREFIX = '/api/v1'

app = FastAPI(title='Billing Service', lifespan=lifespan)
app.include_router(promo.router, prefix=ROUTER_PREFIX)
app.include_router(subscription.router, prefix=ROUTER_PREFIX)
app.include_router(admin.router, prefix=ROUTER_PREFIX)
