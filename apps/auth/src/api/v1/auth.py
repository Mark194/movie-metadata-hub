from fastapi import APIRouter, Depends, HTTPException, status

from api.v1.schemas import RegistrationParams, AuthParams, RefreshParams, LogoutParams, TokenResponse
from core.dependencies import get_auth_service
from services.auth import AuthService

router = APIRouter()


@router.post('/auth/register', status_code=status.HTTP_201_CREATED)
async def register(
    params: RegistrationParams,
    service: AuthService = Depends(get_auth_service),
):
    try:
        result = await service.register(params)
        return result
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post('/auth/login', response_model=TokenResponse)
async def login(
    params: AuthParams,
    service: AuthService = Depends(get_auth_service),
):
    try:
        tokens = await service.login(params)
        return tokens
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))


@router.post('/auth/logout', status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    params: LogoutParams,
    service: AuthService = Depends(get_auth_service),
):
    try:
        import jwt
        from common.settings import get_settings
        settings = get_settings()
        payload = jwt.decode(params.refresh_token, settings.jwt.secret_key, algorithms=[settings.jwt.algorithm])
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid token")
        await service.logout(user_id)
    except jwt.PyJWTError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid token")
    return None


@router.post('/auth/refresh', response_model=TokenResponse)
async def refresh(
    params: RefreshParams,
    service: AuthService = Depends(get_auth_service),
):
    try:
        tokens = await service.refresh(params.refresh_token)
        return tokens
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
