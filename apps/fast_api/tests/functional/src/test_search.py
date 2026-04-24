from http import HTTPStatus

import aiohttp
import pytest

from common.settings import get_settings

settings = get_settings()


async def call_search(params: dict) -> tuple[int, dict | list]:
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{settings.api.url}/api/v1/films/search', params=params) as response:
            body = await response.json()
            return response.status, body


@pytest.mark.parametrize('params, expected_status', [
    ({}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'query': '', 'page_number': 1, 'page_size': 10}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'query': 'test', 'page_number': 0, 'page_size': 10}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'query': 'test', 'page_number': 1, 'page_size': 0}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'query': 'test', 'page_number': 1, 'page_size': 101}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'query': 123, 'page_number': 1, 'page_size': 10}, HTTPStatus.OK),
    ({'query': '; DROP TABLE films; --', 'page_number': 1, 'page_size': 10}, HTTPStatus.OK),
])
@pytest.mark.asyncio
async def test_search_validation(params, expected_status):
    status, _ = await call_search(params)
    assert status == expected_status


@pytest.mark.asyncio
async def test_search_pagination(generate_movies, es_write_data):
    count = 23
    movies = generate_movies(count=count, title='Pagination Test')
    await es_write_data(settings.elastic.index, movies)

    s, b = await call_search({'query': 'Pagination Test', 'page_number': 1, 'page_size': 10})
    assert s == HTTPStatus.OK and len(b) == 10

    s, b = await call_search({'query': 'Pagination Test', 'page_number': 2, 'page_size': 10})
    assert s == HTTPStatus.OK and len(b) == 10

    s, b = await call_search({'query': 'Pagination Test', 'page_number': 3, 'page_size': 10})
    assert s == HTTPStatus.OK and len(b) == 3

    s, b = await call_search({'query': 'Pagination Test', 'page_number': 4, 'page_size': 10})
    assert s == HTTPStatus.OK and len(b) == 0


@pytest.mark.parametrize('query, expected_count', [
    ('The Star', 50),
    ('the star', 50),
    ('star', 50),
    ('Стар', 0),
    ('New World', 50),
])
@pytest.mark.asyncio
async def test_search_phrase(generate_movies, es_write_data, query, expected_count):
    movies = generate_movies(count=50, title='The Star', rating=8.5)
    await es_write_data(settings.elastic.index, movies)

    status, body = await call_search({'query': query, 'page_number': 1, 'page_size': 100})
    assert status == HTTPStatus.OK
    assert len(body) == expected_count


@pytest.mark.asyncio
async def test_search_redis_cache(generate_movies, es_write_data, redis_client):
    movies = generate_movies(count=5, title='Cache Test Movie')
    await es_write_data(settings.elastic.index, movies)

    query = 'Cache Test Movie'
    offset = 0
    limit = 5

    cache_key = f'films:offset:{offset}:limit:{limit}:query:{query}'

    s1, b1 = await call_search({'query': query, 'page_number': 1, 'page_size': limit})
    assert s1 == HTTPStatus.OK
    assert len(b1) == 5

    assert await redis_client.exists(cache_key), f'Key {cache_key} not found in Redis'

    s2, b2 = await call_search({'query': query, 'page_number': 1, 'page_size': limit})
    assert s2 == HTTPStatus.OK
    assert b1 == b2

    cached = await redis_client.get(cache_key)
    assert cached is not None


SEARCH_REQUIRED_FIELDS = {'uuid', 'title', 'imdb_rating'}


@pytest.mark.asyncio
async def test_search_response_schema(generate_movies, es_write_data):
    movies = generate_movies(count=2, title='SchemaTest')
    await es_write_data(settings.elastic.index, movies)

    s, b = await call_search({'query': 'SchemaTest', 'page_number': 1, 'page_size': 2})
    assert s == HTTPStatus.OK
    assert len(b) == 2

    for film in b:
        assert SEARCH_REQUIRED_FIELDS.issubset(film.keys()), f'Missing fields in {film.keys()}'

        assert isinstance(film['uuid'], str)
        assert isinstance(film['title'], str)
        assert isinstance(film['imdb_rating'], (int, float))
