from api.v1.schemas import FilmQueryParams, SearchQueryParams
from fastapi import APIRouter, Depends
from models.film import Film, FilmDetail
from services.film import FilmService
from core.dependencies import get_film_service

router = APIRouter()


@router.get('/films', response_model=list[Film])
async def get_films(
        params=Depends(FilmQueryParams),
        film_service: FilmService = Depends(get_film_service),
):
    return await film_service.get_all(
        sort=params.sort,
        offset=params.offset,
        limit=params.page_size,
        genre=params.genre
    )


@router.get('/films/search', response_model=list[Film])
async def search_films(
        params=Depends(SearchQueryParams),
        film_service: FilmService = Depends(get_film_service),
):
    return await film_service.get_all(
        offset=params.offset,
        limit=params.page_size,
        query=params.query,
    )


@router.get('/films/{film_id}', response_model=FilmDetail)
async def get_film(
        film_id: str,
        film_service: FilmService = Depends(get_film_service),
):
    return await film_service.get_by_id(
        film_id=film_id
    )
