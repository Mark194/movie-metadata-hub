import uuid
from datetime import datetime, timezone

from db.postgres import Base
from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID


class LoginHistory(Base):
    __tablename__ = 'login_history'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    login_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(512), nullable=True)
