import uuid

from db.postgres import Base
from sqlalchemy import UUID, Column, ForeignKey, String
from sqlalchemy.orm import relationship


class SocialAccount(Base):
    __tablename__ = 'social_accounts'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    provider = Column(String(50), nullable=False)  # 'yandex', 'vk'
    provider_user_id = Column(String(255), nullable=False)  # ID пользователя у провайдера
    email = Column(String(255))
    extra_data = Column(String)  # JSON

    user = relationship('User', back_populates='social_accounts')
