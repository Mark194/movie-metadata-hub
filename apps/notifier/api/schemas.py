from typing import Any
from uuid import UUID

from pydantic import BaseModel


class EventPayload(BaseModel):
    event_type: str  # например, "user_registered", "new_movie"
    user_id: UUID | None = None
    movie_id: int | None = None


class BroadcastPayload(BaseModel):
    template_id: int
    context: dict[str, Any] | None = None
    type: str | None = None
    subject: str | None = None
    body: str | None = None


class PersonalizedPayload(BaseModel):
    user_id: UUID
    template_id: int
    context: dict[str, Any] | None = None
    type: str | None = None
    subject: str | None = None
    body: str | None = None


class FreePayload(BaseModel):
    user_id: UUID
    template_id: int | None = None
    subject: str
    text: str
    type: str  # email/sms/push
