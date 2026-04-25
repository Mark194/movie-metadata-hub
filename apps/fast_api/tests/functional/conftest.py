import uuid

import pytest_asyncio
import redis.asyncio as aioredis

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from common import get_settings
from .testdata.es_mapping import MAPPING_MOVIES

settings = get_settings()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_after_test(es_client, redis_client):
    yield
    if await es_client.indices.exists(index=settings.elastic.index):
        await es_client.indices.delete(index=settings.elastic.index)

    await redis_client.flushdb()


@pytest_asyncio.fixture(name='generate_movies')
def generate_movies():
    def inner(
            count: int = 60,
            title: str = 'The Star',
            film_uuid: str | None = None,
            rating: float = 8.5,
            genres: list[dict] | None = None,
    ) -> list[dict]:
        if genres is None:
            genres = [
                {
                    'uuid': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f11',  # ← uuid вместо id
                    'name': 'Action',
                },
                {
                    'uuid': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f22',  # ← uuid вместо id
                    'name': 'Sci-Fi',
                },
            ]
        movies = [
            {
                'uuid': film_uuid if film_uuid is not None else str(uuid.uuid4()),  # ← uuid вместо id
                'title': title,
                'description': 'New World',
                'imdb_rating': rating,
                'genres': genres,
                'actors': [
                    {'uuid': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},  # ← uuid
                    {'uuid': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'},  # ← uuid
                ],
                'writers': [
                    {'uuid': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},  # ← uuid
                    {
                        'uuid': 'b45bd7bc-2e16-46d5-b125-983d356768c6',
                        'name': 'Howard',
                    },
                ],
                'directors': [
                    {'uuid': 'ef00b8ff-3c82-4d31-ad8e-72b69f4e3f00', 'name': 'Stan'},  # ← uuid
                ],
                'actors_names': ['Ann', 'Bob'],
                'writers_names': ['Ben', 'Howard'],
                'directors_names': ['Stan'],
            }
            for _ in range(count)
        ]

        bulk_query: list[dict] = []
        for row in movies:
            data = {'_index': 'movies', '_id': row['uuid']}
            data.update({'_source': row})
            bulk_query.append(data)

        return bulk_query

    return inner


@pytest_asyncio.fixture(name='es_client')
async def es_client():
    es_client = AsyncElasticsearch(hosts=settings.elastic.url, verify_certs=False)
    yield es_client
    await es_client.close()


@pytest_asyncio.fixture(name='es_write_data')
def es_write_data(es_client):
    async def inner(index: str, data: list[dict]):
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)
        await es_client.indices.create(index=index, **MAPPING_MOVIES)

        updated, errors = await async_bulk(
            client=es_client,
            actions=data,
            refresh = 'wait_for'
        )

        if errors:
            raise Exception('Ошибка записи данных в Elasticsearch')

    return inner


@pytest_asyncio.fixture(name='redis_client')
async def redis_client():
    client = aioredis.from_url(settings.redis.url, decode_responses=True)
    yield client

    await client.flushdb()
    await client.aclose()
