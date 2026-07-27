from pathlib import Path

from anyio.functools import lru_cache
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class ElasticSettings(BaseModel):
    url: str
    index: str
    port: int


class RedisSettings(BaseModel):
    host: str
    port: int
    db: int

    @property
    def url(self):
        return f'redis://{self.host}:{self.port}/{self.db}'


class PostgresSettings(BaseModel):
    db: str
    user: str
    password: str
    host: str
    port: int

    @property
    def db_url(self):
        return (f'postgresql://{self.user}:{self.password}@'
                f'{self.host}:{self.port}/{self.db}')

    @property
    def async_db_url(self):
        return (f'postgresql+asyncpg://{self.user}:{self.password}@'
                f'{self.host}:{self.port}/{self.db}')


class MigratorSettings(BaseModel):
    batch_size: int
    sleep_time: int
    storage_type: str


class ApiSettings(BaseModel):
    project_name: str
    url: str


class JWTSettings(BaseModel):
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7


class AuthSettings(BaseModel):
    url: str
    timeout: int
    cache_ttl: int
    with_tracing: bool
    internal_api_token: str


class AdminSettings(BaseModel):
    secret_key: str
    debug: bool


class OAuthProviderSettings(BaseModel):
    client_id: str
    client_secret: str
    redirect_uri: str


class OAuthSettings(BaseModel):
    yandex: OAuthProviderSettings
    vk: OAuthProviderSettings


class NotifierSettings(BaseSettings):
    celery_url: str
    celery_backend: str
    notifier_bd: str


class BillingSettings(BaseModel):
    payment_gateway_url: str
    payment_timeout: int
    default_trial_days: int
    promo_code_length: int


class Settings(BaseSettings):
    app: MigratorSettings
    api: ApiSettings
    auth: AuthSettings
    postgres: PostgresSettings
    redis: RedisSettings
    elastic: ElasticSettings
    jwt: JWTSettings
    admin: AdminSettings
    oauth: OAuthSettings
    notify: NotifierSettings
    billing: BillingSettings

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent / '.env',
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=False,
        env_nested_delimiter='__',
    )


@lru_cache()
def get_settings():
    return Settings()
