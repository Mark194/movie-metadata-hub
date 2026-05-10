from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from uuid import UUID

from models.role import Role
from models.permission import Permission
from models.user import User

ROLE_NOT_FOUND = 'Role not found'
ROLE_OR_PERMISSION_NOT_FOUND = 'Role or permission not found'
ROLE_OR_USER_NOT_FOUND = 'Role or user not found'


class RoleService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_role(self, name: str, description: str = None) -> Role:
        role = Role(name=name, description=description)
        self.db.add(role)
        await self.db.commit()
        await self.db.refresh(role)
        return role

    async def get_roles(self, skip: int = 0, limit: int = 100) -> list[Role]:
        result = await self.db.execute(select(Role).offset(skip).limit(limit))
        return result.scalars().all()

    async def get_role(self, role_id: UUID) -> Role | None:
        return await self.db.get(Role, role_id)

    async def update_role(self, role_id: UUID, name: str = None, description: str = None) -> Role:
        role = await self.get_role(role_id)
        if not role:
            raise ValueError(ROLE_NOT_FOUND)
        if name:
            role.name = name
        if description is not None:
            role.description = description
        await self.db.commit()
        await self.db.refresh(role)
        return role

    async def delete_role(self, role_id: UUID) -> None:
        role = await self.get_role(role_id)
        if role:
            await self.db.delete(role)
            await self.db.commit()

    async def create_permission(self, name: str, resource: str, action: str) -> Permission:
        perm = Permission(name=name, resource=resource, action=action)
        self.db.add(perm)
        await self.db.commit()
        await self.db.refresh(perm)
        return perm

    async def get_permissions(self) -> list[Permission]:
        result = await self.db.execute(select(Permission))
        return result.scalars().all()

    async def assign_permission_to_role(self, role_id: UUID, permission_id: UUID) -> None:
        role = await self.get_role(role_id)
        perm = await self.db.get(Permission, permission_id)
        if not role or not perm:
            raise ValueError(ROLE_OR_PERMISSION_NOT_FOUND)
        if perm not in role.permissions:
            role.permissions.append(perm)
            await self.db.commit()

    async def remove_permission_from_role(self, role_id: UUID, permission_id: UUID) -> None:
        role = await self.get_role(role_id)
        perm = await self.db.get(Permission, permission_id)
        if role and perm and perm in role.permissions:
            role.permissions.remove(perm)
            await self.db.commit()

    async def assign_role_to_user(self, user_id: UUID, role_id: UUID) -> None:
        user = await self.db.get(User, user_id)
        role = await self.get_role(role_id)
        if not user or not role:
            raise ValueError(ROLE_OR_USER_NOT_FOUND)
        if role not in user.roles:
            user.roles.append(role)
            await self.db.commit()

    async def remove_role_from_user(self, user_id: UUID, role_id: UUID) -> None:
        user = await self.db.get(User, user_id)
        role = await self.db.get(Role, role_id)
        if user and role and role in user.roles:
            user.roles.remove(role)
            await self.db.commit()
