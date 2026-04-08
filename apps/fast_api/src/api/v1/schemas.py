from typing import Optional
from fastapi import Query
from pydantic import BaseModel

MINIMAL_PAGE_NUMBER = 1

DEFAULT_PAGE_SIZE = 50
MAXIMUM_PAGE_SIZE = 100


class PaginatedParams(BaseModel):
    page_number: int = Query(1, ge=1)
    page_size: int = Query(50, ge=1, le=100)

    @property
    def offset(self) -> int:
        return (self.page_number - 1) * self.page_size


class FilmQueryParams(PaginatedParams):
    sort: Optional[str] = Query(None, description='Поле для сортировки, используйте \'-\' для обратного порядка')
    genre: Optional[str] = Query(None, description='Фильтр по жанру')


class SearchQueryParams(PaginatedParams):
    query: Optional[str] = Query(None, description='Поиск фильмов по названию')
