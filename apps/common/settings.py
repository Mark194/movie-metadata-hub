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


class MigratorSettings(BaseModel):
    batch_size: int
    sleep_time: int
    storage_type: str


class ApiSettings(BaseModel):
    project_name: str


class Settings(BaseSettings):
    app: MigratorSettings
    api: ApiSettings
    postgres: PostgresSettings
    redis: RedisSettings
    elastic: ElasticSettings

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
