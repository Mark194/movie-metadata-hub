from fastapi import FastAPI
from api.v1 import promo, subscription, admin

ROUTER_PREFIX = '/api/v1'

app = FastAPI(title='Billing Service')
app.include_router(promo.router, prefix=ROUTER_PREFIX)
app.include_router(subscription.router, prefix=ROUTER_PREFIX)
app.include_router(admin.router, prefix=ROUTER_PREFIX)
