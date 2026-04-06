import logging

from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    SQL_HOST: str
    SQL_PORT: int

    ELASTICSEARCH_URL: str
    ELASTICSEARCH_INDEX: str
    ES_PORT: int

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int

    BATCH_SIZE: int
    SLEEP_TIME: int

    STORAGE_TYPE: str

    model_config = SettingsConfigDict(
        env_file='.env',

    )

    def get_db_url(self):
        return (f'postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@'
                f'{self.SQL_HOST}:{self.SQL_PORT}/{self.POSTGRES_DB}')


settings = Settings()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
