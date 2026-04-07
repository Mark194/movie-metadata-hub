from typing import List, Annotated

from fastapi import APIRouter, Depends

from api.v1.schemas import FilmQueryParams
from models.film import Film
from services.film import FilmService, get_film_service

router = APIRouter()


@router.get('/films', response_model=List[Film])
async def get_films(
    params = Depends(FilmQueryParams),  # Переносим класс внутрь Depends
    film_service: FilmService = Depends(get_film_service),
):
    return await film_service.get_all(
        sort=params.sort,
        offset=params.offset,
        limit=params.page_size,
        genre=params.genre,
        query=params.query
    )
