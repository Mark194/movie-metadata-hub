from pydantic import BaseModel


class Film(BaseModel):
    uuid: str
    title: str
    imdb_rating: float | None = None


class Genre(BaseModel):
    uuid: str
    name: str


class Person(BaseModel):
    uuid: str
    name: str


class FilmDetail(BaseModel):
    uuid: str
    title: str
    imdb_rating: float | None = None
    description: str
    genres: list[Genre]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]
