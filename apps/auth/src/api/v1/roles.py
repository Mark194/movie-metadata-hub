from http import HTTPStatus
from uuid import UUID

from api.v1.schemas import PermissionAssign, RoleCreate, RoleResponse, RoleUpdate
from core.dependencies import get_role_service
from fastapi import APIRouter, Depends, HTTPException
from services.role import RoleService

router = APIRouter()

ROLE_NOT_FOUND = 'Role not found'
STATUS_ASSIGNED = {'status': 'assigned'}
STATUS_REMOVED = {'status': 'removed'}


@router.get('/roles', response_model=list[RoleResponse])
async def get_roles(
        skip: int = 0,
        limit: int = 100,
        service: RoleService = Depends(get_role_service)
):
    roles = await service.get_roles(skip, limit)
    return roles


@router.post('/roles', response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def create_role(
        data: RoleCreate,
        service: RoleService = Depends(get_role_service)
):
    try:
        role = await service.create_role(data.name, data.description)
        return role
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e))


@router.get('/roles/{role_id}', response_model=RoleResponse)
async def get_role(
        role_id: UUID,
        service: RoleService = Depends(get_role_service)
):
    role = await service.get_role(role_id)
    if not role:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=ROLE_NOT_FOUND)
    return role


@router.patch('/roles/{role_id}', response_model=RoleResponse)
async def update_role(
        role_id: UUID,
        data: RoleUpdate,
        service: RoleService = Depends(get_role_service)
):
    try:
        role = await service.update_role(role_id, data.name, data.description)
        return role
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e))


@router.get('/roles/{id}')
async def change_role(id):
    pass


@router.delete('/roles/{role_id}', status_code=HTTPStatus.NO_CONTENT)
async def delete_role(
        role_id: UUID,
        service: RoleService = Depends(get_role_service)
):
    await service.delete_role(role_id)


@router.post('/roles/{role_id}/permissions')
async def assign_permission(
        role_id: UUID,
        data: PermissionAssign,
        service: RoleService = Depends(get_role_service)
):
    try:
        await service.assign_permission_to_role(role_id, data.permission_id)
        return STATUS_ASSIGNED
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))


@router.delete('/roles/{role_id}/permissions/{permission_id}')
async def remove_permission(
        role_id: UUID,
        permission_id: UUID,
        service: RoleService = Depends(get_role_service)
):
    await service.remove_permission_from_role(role_id, permission_id)
    return STATUS_REMOVED
