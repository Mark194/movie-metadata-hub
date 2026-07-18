from uuid import UUID

from fastapi import Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from werkzeug.security import generate_password_hash

from common.settings import get_settings
from models.login_history import LoginHistory
from models.role import Role
from models.user import User
from services.cache import CacheService

settings = get_settings()

USER_NOT_FOUND = 'User not found'
INVALID_PASSWORD = 'Invalid password'
INVALID_PASSWORD_OLD = 'Invalid password old'
INVALID_USERNAME = 'Login already taken'
USER_OR_ROLE_NOT_FOUND = 'User or role not found'


class UserService:
    def __init__(self, db: AsyncSession, cache: CacheService):
        self.db = db
        self.cache = cache

    async def change_login(self, user_id: UUID, new_login: str, password: str) -> User:
        user = await self.db.get(User, user_id)
        if not user:
            raise ValueError(USER_NOT_FOUND)
        if not user.check_password(password):
            raise ValueError(INVALID_PASSWORD)

        existing = await self.db.execute(select(User).where(User.login == new_login))
        if existing.scalar_one_or_none():
            raise ValueError(INVALID_USERNAME)

        old_login = user.login
        user.login = new_login
        await self.db.commit()
        await self.db.refresh(user)

        await self.cache.delete(f'refresh:{user_id}')
        return user

    async def change_password(self, user_id: UUID, old_password: str, new_password: str) -> None:
        user = await self.db.get(User, user_id)
        if not user:
            raise ValueError(USER_NOT_FOUND)
        if not user.check_password(old_password):
            raise ValueError(INVALID_PASSWORD_OLD)

        user.password = generate_password_hash(new_password)
        await self.db.commit()
        await self.cache.delete(f'refresh:{user_id}')

    async def record_login_history(self, user_id: UUID, request: Request) -> None:
        client_ip = request.client.host if request.client else None
        user_agent = request.headers.get('user-agent')
        history = LoginHistory(
            user_id=user_id,
            ip_address=client_ip,
            user_agent=user_agent
        )
        self.db.add(history)
        await self.db.commit()

    async def get_login_history(self, user_id: UUID, skip: int = 0, limit: int = 50) -> list[LoginHistory]:
        stmt = (
            select(LoginHistory)
            .where(LoginHistory.user_id == user_id)
            .order_by(LoginHistory.login_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        return result.scalars().all()

    async def assign_role_to_user(self, user_id: UUID, role_id: UUID) -> None:
        user = await self.db.get(User, user_id)
        role = await self.db.get(Role, role_id)
        if not user or not role:
            raise ValueError(USER_OR_ROLE_NOT_FOUND)
        if role not in user.roles:
            user.roles.append(role)
            await self.db.commit()
            await self.cache.delete(f'user_permissions:{user_id}')

    async def remove_role_from_user(self, user_id: UUID, role_id: UUID) -> None:
        user = await self.db.get(User, user_id)
        role = await self.db.get(Role, role_id)
        if user and role and role in user.roles:
            user.roles.remove(role)
            await self.db.commit()
            await self.cache.delete(f'user_permissions:{user_id}')

    async def get_user_by_id(self, user_id: UUID) -> User | None:
        return await self.db.get(User, user_id)

    async def get_all_users(self):
        stmt = (
            select(User)
        )
        result = await self.db.execute(stmt)
        return result.scalars().all()

    async def get_users_paginated(self, limit: int, offset: int) -> list[User]:
        stmt = select(User).offset(offset).limit(limit)
        result = await self.db.execute(stmt)
        return result.scalars().all()
