import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request

HEADER_REQUEST_ID = 'x-request-id'


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(HEADER_REQUEST_ID, str(uuid.uuid4()))
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers[HEADER_REQUEST_ID] = request_id
        return response
