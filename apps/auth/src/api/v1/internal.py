from http import HTTPStatus
from uuid import UUID

from common import get_settings
from core.dependencies import get_user_service
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from services.user import UserService

settings = get_settings()

router = APIRouter(prefix='/internal', tags=['internal'])

@router.get("/users")
async def get_all_users_internal(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    x_internal_token: str = Header(...),
    service: UserService = Depends(get_user_service),
):
    if x_internal_token != settings.INTERNAL_API_TOKEN:
        raise HTTPException(status_code=HTTPStatus.FORBIDDEN, detail="Invalid internal token")
    users = await service.get_users_paginated(limit, offset)
    return {"users": users}

@router.get("/users/{user_id}")
async def get_user_internal(
    user_id: UUID,
    x_internal_token: str = Header(...),
    service: UserService = Depends(get_user_service),
):
    if x_internal_token != settings.INTERNAL_API_TOKEN:
        raise HTTPException(status_code=HTTPStatus.FORBIDDEN, detail="Invalid internal token")
    user = await service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="User not found")
    return user