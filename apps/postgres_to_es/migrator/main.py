import time

from converter.data_converter import DataConverter
from services.elasticsearch_client import ElasticsearchClient
from services.postgres_client import PostgreSQLClient
from services.redis_client import RedisClient
from common import get_settings, get_logger
from storage.json_storage import JsonFileStorage
from storage.redis_storage import RedisStorage

settings = get_settings()
logger = get_logger(__name__)


class MigratorETL:

    def __init__(self):
        if settings.app.storage_type == 'redis':
            self.redis_client = RedisClient(
                settings.redis.host,
                settings.redis.port,
                settings.redis.db
            )
            self.state_storage = RedisStorage(self.redis_client)
        elif settings.app.storage_type == 'json':
            self.state_storage = JsonFileStorage()
        else:
            raise RuntimeError(f'Storage type not supported: {settings.app.storage_type}')

        self.postgres_client = PostgreSQLClient(settings.postgres.db_url)
        self.postgres_client.connect()
        self.es_client = ElasticsearchClient(
            hosts=[settings.elastic.url],
            index=settings.elastic.index,
        )

    def get_last_modified(self) -> str:
        if last_modified := self.state_storage.retrieve_state().get('last_modified'):
            return last_modified

        return '1900-01-01T00:00:00'

    def update_last_modified(self, last_modified: str) -> None:
        state = self.state_storage.retrieve_state()
        state['last_modified'] = last_modified
        self.state_storage.save_state(state)

    def get_affected_movies(self, last_modified: str) -> set:
        affected_movies = set()

        for movie in self.postgres_client.get_updated_movies(last_modified, settings.app.batch_size):
            affected_movies.add(movie['id'])

        person_movies = self.postgres_client.get_updated_persons(last_modified)
        affected_movies.update(person_movies)

        genre_movies = self.postgres_client.get_updated_genres(last_modified)
        affected_movies.update(genre_movies)

        return affected_movies

    def process_movies(self, movies_ids: set) -> bool:

        if not movies_ids:
            return True

        movies = self.postgres_client.get_movies_by_ids(list(movies_ids))

        if not movies:
            return True

        transformed_movies = DataConverter.transform_movies(movies)

        self.es_client.bulk_index(transformed_movies)

        max_modified = max(movie['modified'] for movie in movies)
        self.update_last_modified(max_modified)

        logger.info(f'Processed {len(movies)} movies')
        return True

    def handle(self):
        try:
            last_modified = self.get_last_modified()
            logger.info(f'Start with last modified: {last_modified}')

            if affected_movies := self.get_affected_movies(last_modified):
                logger.info(f'Found affected movies: {len(affected_movies)}')
                self.process_movies(affected_movies)
            else:
                logger.info(f'No affected movies')

        except Exception as err:
            logger.error(f'Found err in handle {err}', exc_info=True)

    def run(self):
        logger.info(f'Start migrator...')

        while True:
            self.handle()
            logger.info(f'Sleep for {settings.app.sleep_time} seconds...')
            time.sleep(settings.app.sleep_time)

    def close(self):
        if self.redis_client:
            self.redis_client.close()

        self.postgres_client.close()


def main():
    migrator = MigratorETL()

    try:
        migrator.run()
    except KeyboardInterrupt:
        logger.info(f'Shutting down migrator...')
    except Exception as err:
        logger.error(err)
    finally:
        migrator.close()


if __name__ == '__main__':
    main()
