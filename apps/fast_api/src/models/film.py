from typing import Optional, Dict, List

from pydantic import BaseModel


class Film(BaseModel):
    uuid: str
    title: str
    imdb_rating: Optional[float] = None


class Genre(BaseModel):
    uuid: str
    name: str

class Person(BaseModel):
    uuid: str
    name: str

class FilmDetail(BaseModel):
    uuid: str
    title: str
    imdb_rating: Optional[float] = None
    description: str
    genres: List[Genre]
    actors: List[Person]
    writers: List[Person]
    directors: List[Person]
