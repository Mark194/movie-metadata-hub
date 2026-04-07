import logging
from functools import lru_cache
from typing import Optional, List, Dict, Any

import orjson
from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import Film

FILM_CACHE_EXPIRE_IN_SECONDS = 300  # 5 минут
FILMS_LIST_CACHE_EXPIRE_IN_SECONDS = 60


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, film_id: str) -> Optional[Film]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        film = await self._film_from_cache(film_id)
        if not film:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            film = await self._get_film_from_elastic(film_id)
            if not film:
                # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
                return None
            # Сохраняем фильм в кеш
            await self._put_film_to_cache(film)

        return film

    async def get_all(
            self,
            sort: Optional[str] = None,
            offset: int = 0,
            limit: int = 50,
            genre: Optional[str] = None,
            query: Optional[str] = None,
    ) -> List[Film]:
        cache_key = self._get_films_cache_key(sort, offset, limit, genre, query)
        films = await self._films_list_from_cache(cache_key)

        if films:
            return films

        # 2. Если нет в кеше - ищем в Elasticsearch
        films = await self._get_films_from_elastic(sort, offset, limit, genre, query)

        # 3. Сохраняем в кеш (даже пустой список, чтобы не ходить в ES при отсутствии данных)
        if films is not None:
            await self._put_films_list_to_cache(cache_key, films)

        return films or []

    async def _get_films_from_elastic(
            self,
            sort: Optional[str] = None,
            offset: int = 0,
            limit: int = 50,
            genre: Optional[str] = None,
            query: Optional[str] = None,
    ) -> Optional[List[Film]]:
        """
        Получение списка фильмов из Elasticsearch
        """
        try:
            body = self._build_elastic_query(sort, offset, limit, genre, query)

            response = await self.elastic.search(
                index='movies',
                body=body
            )

            films = []
            for hit in response['hits']['hits']:
                film_data = hit['_source']
                film_data['uuid'] = hit['_id']
                films.append(Film(**film_data))

            return films

        except (NotFoundError, ESConnectionError) as e:
            logging.error(f'Error fetching films from Elasticsearch: {e}')
            return None

    def _build_elastic_query(
            self,
            sort: Optional[str] = None,
            offset: int = 0,
            limit: int = 50,
            genre: Optional[str] = None,
            query: Optional[str] = None,
    ) -> Dict[str, Any]:

        es_query = {"match_all": {}}

        if query:
            es_query = {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "description", "genre"],  # title важнее
                    "fuzziness": "AUTO"
                }
            }

        filters = []
        if genre:
            filters.append({
                "term": {"genre.keyword": genre}
            })

        body = {
            "from": offset,
            "size": limit,
            "query": es_query,
        }

        if filters:
            body["query"] = {
                "bool": {
                    "must": es_query,
                    "filter": filters
                }
            }

        if sort:
            sort_field = sort
            sort_order = "asc"
            if sort.startswith("-"):
                sort_field = sort[1:]
                sort_order = "desc"

            if sort_field == "imdb_rating":
                body["sort"] = [{sort_field: {"order": sort_order, "missing": "_last"}}]
            else:
                body["sort"] = [{sort_field: {"order": sort_order}}]

        return body

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
        except NotFoundError:
            return None
        return Film(**doc['_source'])

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        # https://redis.io/commands/get/
        data = await self.redis.get(film_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        film = Film.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: Film):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set/
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(film.id, film.json(), FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _films_list_from_cache(self, cache_key: str) -> Optional[List[Film]]:
        """Получение списка фильмов из Redis"""
        data = await self.redis.get(cache_key)
        if not data:
            return None

        # Десериализуем список JSON объектов
        films_data = orjson.loads(data)
        return [Film(**film) for film in films_data]

    async def _put_films_list_to_cache(self, cache_key: str, films: List[Film]):
        """Сохранение списка фильмов в Redis"""
        # Сериализуем список фильмов в JSON
        films_json = orjson.dumps([film.dict() for film in films])
        await self.redis.set(
            cache_key,
            films_json,
            ex=FILMS_LIST_CACHE_EXPIRE_IN_SECONDS
        )

    def _get_films_cache_key(
            self,
            sort: Optional[str] = None,
            offset: int = 0,
            limit: int = 50,
            genre: Optional[str] = None,
            query: Optional[str] = None,
    ) -> str:
        """Генерация ключа для кеша на основе всех параметров запроса"""
        key_parts = ["films", f"offset:{offset}", f"limit:{limit}"]

        if sort:
            key_parts.append(f"sort:{sort}")
        if genre:
            key_parts.append(f"genre:{genre}")
        if query:
            key_parts.append(f"query:{query}")

        return ":".join(key_parts)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
