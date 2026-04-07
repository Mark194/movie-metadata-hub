from typing import Optional

from pydantic import BaseModel


class Film(BaseModel):
    uuid: str
    title: str
    imdb_rating: Optional[float] = None
