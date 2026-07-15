from typing import Any

from pydantic import BaseModel


class EventPayload(BaseModel):
    event_type: str  # например, "user_registered", "new_movie"
    user_id: int | None
    movie_id: int | None


class BroadcastPayload(BaseModel):
    template_id: int
    context: dict[str, Any] | None
    type: str | None
    subject: str | None
    body: str | None


class PersonalizedPayload(BaseModel):
    user_id: int
    template_id: int
    context: dict[str, Any] | None
    type: str | None
    subject: str | None
    body: str | None


class FreePayload(BaseModel):
    user_id: int
    template_id: int | None
    subject: str
    text: str
    type: str  # email/sms/push
