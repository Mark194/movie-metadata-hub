import logging
from functools import lru_cache

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@lru_cache()
def get_logger(name: str):
    return logging.getLogger(name)
