import secrets
from datetime import timedelta

from common import get_settings
from models.social_account import SocialAccount
from models.user import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.auth import AuthService
from services.oauth_providers import get_provider

settings = get_settings()


class OAuthService:
    def __init__(self, db: AsyncSession, auth_service: AuthService):
        self.db = db
        self.auth_service: AuthService = auth_service

    @staticmethod
    def get_auth_url(provider_name: str) -> str:
        return get_provider(provider_name).get_auth_url()

    async def process_callback(self, provider_name: str, code: str):
        provider = get_provider(provider_name)
        # 1. Получаем access_token
        token_data = await provider.get_access_token(code)
        # 2. Получаем данные пользователя
        user_info = await provider.get_user_info(token_data)
        # 3. Находим или создаём пользователя
        user = await self._get_or_create_user(provider_name, user_info)

        access_token, _ = self.auth_service.create_access_token(str(user.id))
        refresh_token = self.auth_service.create_refresh_token(str(user.id))

        refresh_ttl = timedelta(days=settings.jwt.refresh_token_expire_days)
        await self.auth_service.cache.set(
            f"refresh:{user.id}", refresh_token, ttl=int(refresh_ttl.total_seconds())
        )
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
        }

    async def _get_or_create_user(self, provider_name: str, user_info: dict) -> User:
        # Поиск существующей связи
        stmt = select(SocialAccount).where(
            SocialAccount.provider == provider_name,
            SocialAccount.provider_user_id == user_info["provider_user_id"],
        )
        result = await self.db.execute(stmt)
        social_account = result.scalar_one_or_none()
        if social_account:
            return social_account.user

        # Поиск по email (если есть) для привязки
        email = user_info.get("email")
        if email:
            stmt = select(User).where(User.login == email)
            result = await self.db.execute(stmt)
            user = result.scalar_one_or_none()
            if user:
                # Привязываем соц аккаунт к существующему пользователю
                self.db.add(
                    SocialAccount(
                        user_id=user.id,
                        provider=provider_name,
                        provider_user_id=user_info["provider_user_id"],
                        email=email,
                    )
                )
                await self.db.commit()
                return user

        random_password = secrets.token_urlsafe(32)
        login = email if email else f"{provider_name}_{user_info['provider_user_id']}"
        user = User(
            login=login,
            password=random_password,
            first_name=user_info.get("first_name", ""),
            last_name=user_info.get("last_name", ""),
        )
        self.db.add(user)
        await self.db.flush()
        self.db.add(
            SocialAccount(
                user_id=user.id,
                provider=provider_name,
                provider_user_id=user_info["provider_user_id"],
                email=email,
            )
        )
        await self.db.commit()
        return user
