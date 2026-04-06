from contextlib import contextmanager
from typing import Dict, List, Optional, Any

import psycopg2
from psycopg2.extras import DictCursor

from settings import logger
from utils.query_loader import QueryLoader


class PostgreSQLClient:

    def __init__(self, url: str):
        self.url = url
        self.queries = QueryLoader()

    def connect(self):
        try:
            self.connection = psycopg2.connect(self.url, cursor_factory=DictCursor)
            logger.info('Connected to PostgreSQL')
        except Exception as err:
            logger.error(f'Failed to connect to PostgreSQL: {err}')
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info('Disconnected from PostgreSQL')

    @contextmanager
    def get_cursor(self):
        if not self.connection or self.connection.closed:
            self.connect()

        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as err:
            logger.error(f'Failed to commit to PostgreSQL: {err}')
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def get_updated_movies(self, last_modified: Optional[str] = None, limit: int = 1000):
        query = self.queries.load('movies/get_updated_movies')
        with self.get_cursor() as cursor:
            cursor.execute(query, {
                'last_modified': last_modified or '1900-01-01',
                'limit': limit
            })
            results = cursor.fetchall()

        return [dict(row) for row in results]

    def get_updated_persons(self, last_modified: Optional[str] = None):
        query = self.queries.load('persons/get_updated_persons')
        with self.get_cursor() as cursor:
            cursor.execute(query, {'last_modified': last_modified or '1900-01-01'})
            results = cursor.fetchall()
        return [row['id'] for row in results]

    def get_updated_genres(self, last_modified: Optional[str] = None):
        query = self.queries.load('genres/get_updated_genres')
        with self.get_cursor() as cursor:
            cursor.execute(query, {'last_modified': last_modified or '1900-01-01'})
            results = cursor.fetchall()
        return [row['id'] for row in results]

    def get_movies_by_ids(self, ids: List[int]) -> List[Dict[str, Any]]:
        query = self.queries.load('movies/get_movies_by_ids')
        with self.get_cursor() as cursor:
            cursor.execute(query, {'ids': ids})
            results = cursor.fetchall()
        return [dict(row) for row in results]
