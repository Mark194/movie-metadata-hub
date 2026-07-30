from http import HTTPStatus

from core.dependencies import get_oauth_service
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from services.oauth import OAuthService

router = APIRouter()


@router.get("/login/{provider}")
async def oauth_login(
    provider: str, oauth_service: OAuthService = Depends(get_oauth_service)
):
    try:
        auth_url = oauth_service.get_auth_url(provider)
        return RedirectResponse(auth_url)
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))


@router.get("/callback/{provider}")
async def oauth_callback(
    provider: str,
    code: str,
    oauth_service: OAuthService = Depends(get_oauth_service),
):
    try:
        tokens = await oauth_service.process_callback(provider, code)
        return tokens
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))
