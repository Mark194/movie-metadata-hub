from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from common import get_logger
from utils.index_loader import load_index_from_json

logger = get_logger(__name__)


class ElasticsearchClient:

    def __init__(self, hosts: list[str], index: str = 'movies'):
        self.client = Elasticsearch(hosts)
        self.index = index
        self._ensure_index()

        logger.info('Connected to Elasticsearch')

    def _ensure_index(self):
        mapping = load_index_from_json('data/index/index.json')

        if not self.client.indices.exists(index=self.index):
            self.client.indices.create(index=self.index, body=mapping)
            logger.info(f'Created index {self.index}')

    def bulk_index(self, movies: list[dict[str, Any]]):
        if not movies:
            return

        actions = [
            {
                '_index': self.index,
                '_id': movie['uuid'],
                '_source': movie
            }
            for movie in movies
        ]

        try:
            success, failed = bulk(self.client, actions, stats_only=False, raise_on_error=False)

            if failed:
                logger.error(f'Failed to index {len(failed)} movies')
                for idx, error in enumerate(failed[:10]):
                    logger.error(f'Error {idx + 1}: {error}')

            logger.info(f'Indexed {success} movies, failed: {len(failed) if isinstance(failed, list) else failed}')

        except BulkIndexError as e:
            logger.error(f'Bulk indexing error: {e}')
            for idx, error in enumerate(e.errors[:10]):
                logger.error(f'Error {idx + 1}: {error}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during bulk indexing: {e}')
            raise

    def delete_movies(self, ids: list[str]):
        if not ids:
            return

        for movie_id in ids:
            try:
                self.client.delete(index=self.index, id=movie_id, ignore=[400, 404])
            except Exception as err:
                logger.error(f'Failed to delete movie {movie_id}')
