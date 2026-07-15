from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from api import notify
from common import get_logger

app = FastAPI(
    title='Notification Service API',
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)
logger = get_logger(__name__)

app.include_router(notify.router, prefix='/api', tags=['notify'])
