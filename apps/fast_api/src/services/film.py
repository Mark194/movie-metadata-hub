import logging

from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.exceptions import ConnectionError as ESConnectionError

from core.cache_keys import CacheKeyBuilder
from fastapi import HTTPException
from models.film import Film, FilmDetail
from services.cache import CacheService
from utils.elastic_builder import ElasticQueryBuilder


class FilmService:
    FILM_CACHE_EXPIRE = 300  # 5 минут
    FILMS_LIST_CACHE_EXPIRE = 60  # 1 минута

    def __init__(
            self,
            cache_service: CacheService,
            elastic: AsyncElasticsearch
    ):
        self.cache = cache_service
        self.elastic = elastic
        self.query_builder = ElasticQueryBuilder()
        self.key_builder = CacheKeyBuilder()

    async def get_by_id(self, film_id: str) -> FilmDetail | None:
        cache_key = self.key_builder.film_detail(film_id)

        film = await self.cache.get(cache_key, FilmDetail)
        if film:
            return film

        film = await self._fetch_film_from_elastic(film_id)
        if not film:
            return None

        await self.cache.set(cache_key, film, self.FILM_CACHE_EXPIRE)

        return film

    async def get_all(
            self,
            sort: str | None = None,
            offset: int = 0,
            limit: int = 50,
            genre: str | None = None,
            query: str | None = None,
    ) -> list[Film]:
        cache_key = self.key_builder.films_list(sort, offset, limit, genre, query)

        films = await self.cache.get_list(cache_key, Film)
        if films is not None:
            return films

        films = await self._fetch_films_from_elastic(sort, offset, limit, genre, query)

        if films:
            await self.cache.set_list(cache_key, films, self.FILMS_LIST_CACHE_EXPIRE)

        return films or []

    async def _fetch_film_from_elastic(self, film_id: str) -> FilmDetail | None:
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
            return FilmDetail(**doc['_source'])
        except NotFoundError:
            raise HTTPException(status_code=404, detail=f"Film {film_id} not found")
        except ESConnectionError as e:
            raise HTTPException(status_code=404, detail=f"Elasticsearch connection error: {e}")


    async def _fetch_films_from_elastic(
            self,
            sort: str | None = None,
            offset: int = 0,
            limit: int = 50,
            genre: str | None = None,
            query: str | None = None,
    ) -> list[Film]:
        try:
            body = self.query_builder.build_films_query(sort, offset, limit, genre, query)
            response = await self.elastic.search(index='movies', body=body)

            return [
                Film(**hit['_source'])
                for hit in response['hits']['hits']
            ]
        except (NotFoundError, ESConnectionError) as e:
            logging.error(f'Error fetching films from Elasticsearch: {e}')
            return []
