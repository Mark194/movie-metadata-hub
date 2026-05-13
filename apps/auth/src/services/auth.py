import uuid
from typing import Any

import jwt

from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from api.v1.schemas import RegistrationParams, AuthParams
from common.settings import get_settings
from models.user import User
from services.cache import CacheService

settings = get_settings()

INVALID_TOKEN_PAYLOAD = 'Invalid token payload'
INVALID_LOGIN_OR_PASSWORD = 'Invalid login or password'

USER_ALREADY_EXISTS = 'User already exists'


class AuthService:
    def __init__(self, cache: CacheService, db: AsyncSession):
        self.cache = cache
        self.db = db

    async def register(self, params: RegistrationParams) -> dict:
        stmt = select(User).where(User.login == params.login)
        result = await self.db.execute(stmt)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            raise ValueError(USER_ALREADY_EXISTS)

        user = User(
            login=params.login,
            password=params.password,
            first_name=params.first_name,
            last_name=params.last_name,
        )
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)

        return {'id': str(user.id), 'login': user.login}

    async def login(self, params: AuthParams) -> tuple[dict[str, tuple[str, str] | str], Any | None]:
        stmt = select(User).where(User.login == params.login)
        result = await self.db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user or not user.check_password(params.password):
            raise ValueError(INVALID_LOGIN_OR_PASSWORD)

        access_token = self._create_access_token(user_id=str(user.id))
        refresh_token = self._create_refresh_token(user_id=str(user.id))

        refresh_ttl = timedelta(days=settings.jwt.refresh_token_expire_days)
        await self.cache.set(
            key=f'refresh:{user.id}',
            value=refresh_token,
            ttl=int(refresh_ttl.total_seconds())
        )

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'bearer'
        }, user

    async def logout(self, access_token: str, refresh_token: str) -> None:
        try:
            payload = jwt.decode(
                access_token,
                settings.jwt.secret_key,
                algorithms=[settings.jwt.algorithm]
            )
            jti = payload.get('jti')
            exp = payload.get('exp')
            if not jti or not exp:
                raise ValueError('Invalid access token payload')
        except jwt.PyJWTError as e:
            raise ValueError(f'Invalid access token: {e}')

        now = datetime.utcnow().timestamp()
        ttl = max(0, int(exp - now))

        if ttl > 0:
            await self.cache.set(f'blacklist:{jti}', '1', ttl=ttl)

        user_id = payload.get('sub')
        if user_id:
            await self.cache.delete(f'refresh:{user_id}')
        else:
            try:
                refresh_payload = jwt.decode(
                    refresh_token,
                    settings.jwt.secret_key,
                    algorithms=[settings.jwt.algorithm]
                )

                if user_id := refresh_payload.get('sub'):
                    await self.cache.delete(f'refresh:{user_id}')
            except jwt.PyJWTError as e:
                raise ValueError(f'Invalid refresh token: {e}')

    async def refresh(self, refresh_token: str) -> dict:
        try:
            payload = jwt.decode(
                refresh_token,
                settings.jwt.secret_key,
                algorithms=[settings.jwt.algorithm]
            )
            user_id = payload.get('sub')
            if not user_id:
                raise ValueError('Invalid token')
        except jwt.PyJWTError:
            raise ValueError('Invalid or expired refresh token')

        stored_token = await self.cache.get(f'refresh:{user_id}')

        if stored_token is not None and isinstance(stored_token, bytes):
            stored_token = stored_token.decode('utf-8')

        if stored_token != refresh_token:
            raise ValueError('Refresh token not found or already used')

        new_access = self._create_access_token(user_id)
        new_refresh = self._create_refresh_token(user_id)

        refresh_ttl = timedelta(days=settings.jwt.refresh_token_expire_days)
        await self.cache.set(
            key=f'refresh:{user_id}',
            value=new_refresh,
            ttl=int(refresh_ttl.total_seconds())
        )

        return {
            'access_token': new_access,
            'refresh_token': new_refresh,
            'token_type': 'bearer'
        }

    def _create_access_token(self, user_id: str) -> tuple[str, str]:
        jti = str(uuid.uuid4())
        expire = datetime.utcnow() + timedelta(minutes=settings.jwt.access_token_expire_minutes)
        payload = {
            'sub': user_id,
            'exp': expire,
            'jti': jti,
            'type': 'access'
        }
        token = jwt.encode(payload, settings.jwt.secret_key, algorithm=settings.jwt.algorithm)
        return token

    def _create_refresh_token(self, user_id: str) -> str:
        expire = datetime.utcnow() + timedelta(days=settings.jwt.refresh_token_expire_days)
        payload = {'sub': user_id, 'exp': expire, 'type': 'refresh'}
        return jwt.encode(payload, settings.jwt.secret_key, algorithm=settings.jwt.algorithm)
