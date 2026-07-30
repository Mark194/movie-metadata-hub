from http import HTTPStatus
from uuid import UUID

from core.dependencies import (
    get_current_user,
    get_user_service,
    require_permission,
)
from fastapi import APIRouter, Depends, HTTPException, status
from models.user import User
from services.user import UserService

from api.v1.schemas import (
    ChangeLoginParams,
    ChangePasswordParams,
    LoginHistoryResponse,
    RoleAssignParams,
    UserOut,
)

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
        return
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
        return
    except ValueError:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=INVALID_USER_OR_ROLE)


@router.get('/users', response_model=list[UserOut])
async def get_all_users(service: UserService = Depends(get_user_service)):
    users = await service.get_users()
    return users


@router.get('/users/{user_id}', response_model=UserOut)
async def get_user(
        user_id: UUID,
        _: User = Depends(require_permission('roles:revoke')),
        service: UserService = Depends(get_user_service)
):
    user = await service.get_user(user_id)
    if not user:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='User not found')
    return user
