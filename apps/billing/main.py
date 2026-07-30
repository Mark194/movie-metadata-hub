from contextlib import asynccontextmanager

from api.v1 import admin, promo, subscription
from external_services.clients import auth_client, payment_client
from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Код, выполняемый при старте
    yield
    # Код, выполняемый при завершении
    await auth_client.aclose()
    await payment_client.aclose()


ROUTER_PREFIX = '/api/v1'

app = FastAPI(title='Billing Service', lifespan=lifespan)
app.include_router(promo.router, prefix=ROUTER_PREFIX)
app.include_router(subscription.router, prefix=ROUTER_PREFIX)
app.include_router(admin.router, prefix=ROUTER_PREFIX)
