from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, status
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from api.v1.schemas import ChangeLoginParams, ChangePasswordParams, LoginHistoryResponse, RoleAssignParams
from core.dependencies import get_current_user, get_postgres, get_cache_service, require_permission, get_user_service
from services.user import UserService
from services.cache import CacheService
from models.user import User

router = APIRouter()

PASSWORD_CHANGED = {'message': 'Password changed successfully'}
REQUIRED_ADMIN_ROLE = 'Admin role required'
ADMIN_ROLE = 'admin'
INVALID_USER_OR_ROLE = 'Invalid user or role'


@router.put('/user/login')
async def change_login(
        params: ChangeLoginParams,
        current_user: User = Depends(get_current_user),
        service: UserService = Depends(get_user_service),
):
    try:
        updated_user = await service.change_login(current_user.id, params.new_login, params.password)
        return {'login': updated_user.login}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.put('/user/password')
async def change_password(
        params: ChangePasswordParams,
        current_user: User = Depends(get_current_user),
        service: UserService = Depends(get_user_service),
):
    try:
        await service.change_password(current_user.id, params.old_password, params.new_password)
        return PASSWORD_CHANGED
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get('/user/login-history', response_model=list[LoginHistoryResponse])
async def get_login_history(
        current_user: User = Depends(get_current_user),
        skip: int = 0,
        limit: int = 50,
        service: UserService = Depends(get_user_service),
):
    history = await service.get_login_history(current_user.id, skip, limit)
    return history


@router.post('/users/{user_id}/roles', status_code=status.HTTP_204_NO_CONTENT)
async def assign_role_to_user(
        user_id: UUID,
        params: RoleAssignParams,
        _: User = Depends(require_permission('roles:assign')),
        service: UserService = Depends(get_user_service),
):
    try:
        await service.assign_role_to_user(user_id, params.role_id)
        return None
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))


@router.delete('/users/{user_id}/roles/{role_id}', status_code=status.HTTP_204_NO_CONTENT)
async def remove_role_from_user(
        user_id: UUID,
        role_id: UUID,
        _: User = Depends(require_permission('roles:revoke')),
        service: UserService = Depends(get_user_service),
):
    try:
        await service.remove_role_from_user(user_id, role_id)
        return None
    except ValueError:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=INVALID_USER_OR_ROLE)
