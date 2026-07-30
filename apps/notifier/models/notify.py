import enum
import uuid

from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class NotificationType(str, enum.Enum):
    EMAIL = 'email'
    SMS = 'sms'
    PUSH = 'push'


class NotificationStatus(str, enum.Enum):
    PENDING = 'pending'
    SENT = 'sent'
    FAILED = 'failed'


class Template(Base):
    __tablename__ = 'templates'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    subject_template = Column(String(200), nullable=False)
    body_template = Column(Text, nullable=False)
    type = Column(Enum(NotificationType), nullable=False)


class Notification(Base):
    __tablename__ = 'notifications'
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False)
    template_id = Column(Integer, ForeignKey('templates.id'), nullable=True)
    subject = Column(String(200), nullable=True)  # финальный сабжект
    body = Column(Text, nullable=True)  # финальный текст
    type = Column(Enum(NotificationType), nullable=False)
    status = Column(Enum(NotificationStatus), default=NotificationStatus.PENDING)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    sent_at = Column(DateTime(timezone=True), nullable=True)
