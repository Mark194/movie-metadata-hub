import redis
from typing import Optional

from settings import logger


class RedisClient:

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._test_connection()

    def _test_connection(self):
        try:
            self.client.ping()
            logger.info('Connected to Redis')
        except Exception as e:
            logger.error(f'Failed to connect to Redis: {e}')
            raise

    def get(self, key: str) -> Optional[str]:
        return self.client.get(key)

    def set(self, key: str, value: str, expire: Optional[int] = None):
        self.client.set(key, value, ex=expire)

    def close(self):
        self.client.close()
