# tests/functional/src/test_film_detail.py
"""
Функциональные тесты для эндпоинта GET /films/{film_id}
Стиль: AAA (Arrange-Act-Assert) с переносами между логическими блоками
"""

import uuid
from http import HTTPStatus

import aiohttp
import pytest

from common.settings import get_settings

settings = get_settings()


async def call_film_detail(film_id: str) -> tuple[int, dict | list | str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(
                f'{settings.api.url}/api/v1/films/{film_id}'
        ) as resp:
            content_type = resp.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                body = await resp.json()
            else:
                body = await resp.text()
            return resp.status, body


@pytest.mark.parametrize('film_id, expected_status', [
    ('not-a-uuid', HTTPStatus.UNPROCESSABLE_ENTITY),
    ('123', HTTPStatus.UNPROCESSABLE_ENTITY),
    ('xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx', HTTPStatus.UNPROCESSABLE_ENTITY),
    ('ABC12345-1234-5678-1234-567812345678', HTTPStatus.UNPROCESSABLE_ENTITY),
    ('00000000-0000-0000-0000-000000000000', HTTPStatus.NOT_FOUND),
    ('ffffffff-ffff-ffff-ffff-ffffffffffff', HTTPStatus.NOT_FOUND),
    ('12345678-1234-5678-1234-567812345678', HTTPStatus.OK),
])
@pytest.mark.asyncio
async def test_film_detail_validation(film_id, expected_status, generate_movies, es_write_data):
    if expected_status == HTTPStatus.OK:
        movie = generate_movies(count=1, film_uuid=film_id, title='ValidationTest')
        await es_write_data(settings.elastic.index, movie)

    status, body = await call_film_detail(film_id)

    assert status == expected_status

    if status == HTTPStatus.UNPROCESSABLE_ENTITY:
        assert 'detail' in body
        assert isinstance(body['detail'], list)

    if status == HTTPStatus.NOT_FOUND:
        if isinstance(body, dict) and 'detail' in body:
            assert 'not found' in body['detail'].lower() or 'film' in body['detail'].lower()


@pytest.mark.asyncio
async def test_film_detail_success(generate_movies, es_write_data):
    film_uuid = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    genre_uuid = '11111111-2222-3333-4444-555555555555'

    movie = generate_movies(
        count=1,
        film_uuid=film_uuid,
        title='The Complete Film',
        rating=9.2,
        genres=[{'uuid': genre_uuid, 'name': 'Sci-Fi'}]
    )
    await es_write_data(settings.elastic.index, movie)

    status, film = await call_film_detail(film_uuid)

    assert status == HTTPStatus.OK
    assert isinstance(film, dict)

    required_fields = {
        'uuid', 'title', 'imdb_rating', 'description',
        'genres', 'actors', 'writers', 'directors'
    }
    assert required_fields.issubset(film.keys()), f'Missing: {required_fields - film.keys()}'

    assert film['uuid'] == film_uuid
    assert film['title'] == 'The Complete Film'
    assert film['imdb_rating'] == 9.2
    assert film['description'] == 'New World'

    assert isinstance(film['genres'], list)
    assert len(film['genres']) == 1
    assert film['genres'][0]['uuid'] == genre_uuid
    assert film['genres'][0]['name'] == 'Sci-Fi'

    for field in ['actors', 'writers', 'directors']:
        assert isinstance(film[field], list)
        assert len(film[field]) > 0
        for person in film[field]:
            assert 'uuid' in person and 'name' in person
            assert isinstance(person['uuid'], str)
            assert isinstance(person['name'], str)


@pytest.mark.asyncio
async def test_film_detail_redis_cache(generate_movies, es_write_data, redis_client):
    film_uuid = 'ca5e7e12-3456-7890-abcd-ef1234567890'
    movie = generate_movies(count=1, film_uuid=film_uuid, title='CacheDetailTest')
    await es_write_data(settings.elastic.index, movie)
    cache_key = f'film:{film_uuid}'

    status1, film1 = await call_film_detail(film_uuid)

    assert status1 == HTTPStatus.OK
    assert await redis_client.exists(cache_key), f'Cache key {cache_key} not found after first request'

    status2, film2 = await call_film_detail(film_uuid)

    assert status2 == HTTPStatus.OK
    assert film1 == film2

    cached_raw = await redis_client.get(cache_key)
    assert cached_raw is not None

    other_uuid = 'db6f8f23-4567-8901-bcde-f12345678901'
    other_movie = generate_movies(count=1, film_uuid=other_uuid, title='OtherCacheTest')
    await es_write_data(settings.elastic.index, other_movie)
    other_cache_key = f'film:{other_uuid}'

    status3, film3 = await call_film_detail(other_uuid)

    assert status3 == HTTPStatus.OK
    assert await redis_client.exists(other_cache_key), 'New cache key not created for different film_id'
    assert not await redis_client.exists(f'film:{film_uuid}_wrong'), 'Wrong key pattern detected'


@pytest.mark.asyncio
async def test_film_detail_response_schema(generate_movies, es_write_data):
    film_uuid = str(uuid.uuid4())
    movie = generate_movies(count=1, film_uuid=film_uuid, title='SchemaDetailTest')
    await es_write_data(settings.elastic.index, movie)

    status, film = await call_film_detail(film_uuid)

    assert status == HTTPStatus.OK

    assert isinstance(film['uuid'], str)
    assert isinstance(film['title'], str)
    assert isinstance(film['imdb_rating'], (int, float, type(None)))
    assert isinstance(film['description'], str)

    assert isinstance(film['genres'], list)
    for genre in film['genres']:
        assert isinstance(genre, dict)
        assert 'uuid' in genre and 'name' in genre
        assert isinstance(genre['uuid'], str)
        assert isinstance(genre['name'], str)

    for field in ['actors', 'writers', 'directors']:
        assert isinstance(film[field], list)
        for person in film[field]:
            assert isinstance(person, dict)
            assert 'uuid' in person and 'name' in person
            assert isinstance(person['uuid'], str)
            assert isinstance(person['name'], str)


@pytest.mark.asyncio
async def test_film_detail_idempotency(generate_movies, es_write_data):
    film_uuid = str(uuid.uuid4())
    movie = generate_movies(count=1, film_uuid=film_uuid, title='IdempotencyTest', rating=7.7)
    await es_write_data(settings.elastic.index, movie)

    responses = []
    for _ in range(5):
        status, film = await call_film_detail(film_uuid)
        assert status == HTTPStatus.OK
        responses.append(film)

    for i in range(1, len(responses)):
        assert responses[0] == responses[i], f'Response {i} differs from first'

    assert all(f['imdb_rating'] == 7.7 for f in responses)
    assert all(f['title'] == 'IdempotencyTest' for f in responses)
