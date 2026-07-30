from http import HTTPStatus

from core.dependencies import get_access_token, get_auth_service
from core.rate_limit import limiter
from fastapi import APIRouter, Depends, HTTPException, Request, status
from services.auth import AuthService
from services.user import UserService

from api.v1.schemas import (
    AuthParams,
    LogoutParams,
    RefreshParams,
    RegistrationParams,
    TokenResponse,
)
from api.v1.users import get_user_service

router = APIRouter()


@router.post("/auth/register", status_code=status.HTTP_201_CREATED)
@limiter.limit("10/minute")
async def register(
    request: Request,
    params: RegistrationParams,
    service: AuthService = Depends(get_auth_service),
):
    try:
        result = await service.register(params)
        return result
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/auth/login", response_model=TokenResponse)
@limiter.limit("5/minute")
async def login(
    params: AuthParams,
    request: Request,
    auth_service: AuthService = Depends(get_auth_service),
    user_service: UserService = Depends(get_user_service),
):
    try:
        tokens, user = await auth_service.login(params)
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail=str(e))

    await user_service.record_login_history(user.id, request)

    return tokens


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    params: LogoutParams,
    access_token: str = Depends(get_access_token),
    service: AuthService = Depends(get_auth_service),
):
    try:
        await service.logout(access_token, params.refresh_token)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh(
    params: RefreshParams,
    service: AuthService = Depends(get_auth_service),
):
    try:
        tokens = await service.refresh(params.refresh_token)
        return tokens
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
