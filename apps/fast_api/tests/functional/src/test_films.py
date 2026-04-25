import pytest
import aiohttp
from http import HTTPStatus

import pytest_asyncio
from common.settings import get_settings

settings = get_settings()


async def call_films(params: dict) -> tuple[int, list | dict]:
    async with aiohttp.ClientSession() as session:
        async with session.get(
                f'{settings.api.url}/api/v1/films',
                params=params
        ) as resp:
            body = await resp.json()
            return resp.status, body


def build_films_cache_key(
        offset: int = 0,
        limit: int = 50,
        sort: str | None = None,
        genre: str | None = None
) -> str:
    key_parts = ['films', f'offset:{offset}', f'limit:{limit}']
    if sort:
        key_parts.append(f'sort:{sort}')
    if genre:
        key_parts.append(f'genre:{genre}')
    return ':'.join(key_parts)


@pytest.mark.parametrize('params, expected_status', [
    ({}, HTTPStatus.OK),
    ({'page_number': 0, 'page_size': 10}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'page_number': -5, 'page_size': 10}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'page_number': 1, 'page_size': 0}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'page_number': 1, 'page_size': 101}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'page_number': 'abc', 'page_size': 10}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'page_number': 1, 'page_size': 10.5}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({'sort': 'imdb_rating'}, HTTPStatus.OK),
    ({'sort': '-imdb_rating'}, HTTPStatus.OK),
    ({'genre': '00000000-0000-0000-0000-000000000001'}, HTTPStatus.OK),
])
@pytest.mark.asyncio
async def test_films_validation(params, expected_status):
    status, body = await call_films(params)

    assert status == expected_status

    if status == HTTPStatus.UNPROCESSABLE_ENTITY:
        assert 'detail' in body
        assert isinstance(body['detail'], list)


@pytest_asyncio.fixture(scope='function')
async def pagination_test_data(generate_movies, es_write_data):
    movies = generate_movies(count=15, title='PaginationTest')
    await es_write_data(settings.elastic.index, movies)
    yield


@pytest.mark.parametrize('query_params, expected_length', [
    ({}, 15),
    ({'page_number': 1, 'page_size': 5}, 5),
    ({'page_number': 2, 'page_size': 5}, 5),
    ({'page_number': 3, 'page_size': 5}, 5),
    ({'page_number': 4, 'page_size': 5}, 0),
])
@pytest.mark.asyncio
async def test_films_pagination_and_defaults(pagination_test_data, query_params, expected_length):
    status, body = await call_films(query_params)
    assert status == HTTPStatus.OK and len(body) == expected_length


@pytest.mark.asyncio
async def test_films_filter_and_specific_retrieval(generate_movies, es_write_data):
    genre_uuid_1 = 'aa11bb22-cc33-dd44-ee55-ff6677889901'
    genre_uuid_2 = 'aa11bb22-cc33-dd44-ee55-ff6677889902'

    action_movie = generate_movies(
        count=1, title='The Matrix', rating=9.5,
        genres=[{'uuid': genre_uuid_1, 'name': 'Action'}]
    )
    drama_movies = [
        generate_movies(count=1, title='Drama One', rating=8.0, genres=[{'uuid': genre_uuid_2, 'name': 'Drama'}])[0],
        generate_movies(count=1, title='Drama Two', rating=7.5, genres=[{'uuid': genre_uuid_2, 'name': 'Drama'}])[0],
    ]
    await es_write_data(settings.elastic.index, action_movie + drama_movies)

    s, b = await call_films({'genre': genre_uuid_1, 'page_size': 10})
    assert s == HTTPStatus.OK
    assert len(b) == 1
    assert b[0]['title'] == 'The Matrix'

    s, b = await call_films({'sort': '-imdb_rating', 'page_size': 10})
    assert b[0]['imdb_rating'] == 9.5
    assert b[1]['imdb_rating'] == 8.0
    assert b[2]['imdb_rating'] == 7.5

    s, b = await call_films({'genre': genre_uuid_2, 'sort': 'imdb_rating', 'page_size': 10})
    assert len(b) == 2
    assert b[0]['title'] == 'Drama Two'
    assert b[1]['title'] == 'Drama One'


@pytest.mark.asyncio
async def test_films_redis_cache(generate_movies, es_write_data, redis_client):
    movies = generate_movies(count=4, title='CacheTestMovie')
    await es_write_data(settings.elastic.index, movies)

    params = {'page_number': 1, 'page_size': 4, 'sort': '-imdb_rating'}
    offset = 0
    limit = 4
    cache_key = build_films_cache_key(offset=offset, limit=limit, sort='-imdb_rating')

    s1, b1 = await call_films(params)

    assert s1 == HTTPStatus.OK
    assert len(b1) == 4
    assert await redis_client.exists(cache_key), f'Cache key {cache_key} not found'

    s2, b2 = await call_films(params)

    assert s2 == HTTPStatus.OK
    assert b1 == b2

    params_diff = {'page_number': 2, 'page_size': 2}
    cache_key_diff = build_films_cache_key(offset=2, limit=2)

    await call_films(params_diff)

    assert await redis_client.exists(cache_key_diff), 'New cache key not created for different params'


REQUIRED_FILM_FIELDS = {'uuid', 'title', 'imdb_rating'}


@pytest.mark.asyncio
async def test_films_response_schema(generate_movies, es_write_data):
    movies = generate_movies(count=2, title='SchemaCheck')
    await es_write_data(settings.elastic.index, movies)

    s, b = await call_films({'page_size': 2})

    assert s == HTTPStatus.OK
    assert len(b) == 2

    for film in b:
        assert REQUIRED_FILM_FIELDS.issubset(film.keys()), f'Missing fields: {REQUIRED_FILM_FIELDS - film.keys()}'

        assert isinstance(film['uuid'], str)
        assert isinstance(film['title'], str)
        assert isinstance(film['imdb_rating'], (int, float, type(None)))  # float | None
