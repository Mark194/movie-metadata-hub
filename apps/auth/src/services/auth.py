import jwt

from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from api.v1.schemas import RegistrationParams, AuthParams
from common.settings import get_settings
from models.user import User
from services.cache import CacheService

settings = get_settings()


class AuthService:
    def __init__(self, cache: CacheService, db: AsyncSession):
        self.cache = cache
        self.db = db

    async def register(self, params: RegistrationParams) -> dict:
        stmt = select(User).where(User.login == params.login)
        result = await self.db.execute(stmt)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            raise ValueError('User already exists')

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

    async def login(self, params: AuthParams) -> dict:
        stmt = select(User).where(User.login == params.login)
        result = await self.db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user or not user.check_password(params.password):
            raise ValueError('Invalid login or password')

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
        }

    async def logout(self, user_id: str) -> None:
        await self.cache.delete(f'refresh:{user_id}')

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

    def _create_access_token(self, user_id: str) -> str:
        expire = datetime.utcnow() + timedelta(minutes=settings.jwt.access_token_expire_minutes)
        payload = {'sub': user_id, 'exp': expire, 'type': 'access'}
        return jwt.encode(payload, settings.jwt.secret_key, algorithm=settings.jwt.algorithm)

    def _create_refresh_token(self, user_id: str) -> str:
        expire = datetime.utcnow() + timedelta(days=settings.jwt.refresh_token_expire_days)
        payload = {'sub': user_id, 'exp': expire, 'type': 'refresh'}
        return jwt.encode(payload, settings.jwt.secret_key, algorithm=settings.jwt.algorithm)
