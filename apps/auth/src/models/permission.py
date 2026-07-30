import uuid

from db.postgres import Base
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship


class Permission(Base):
    __tablename__ = 'permissions'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), unique=True, nullable=False)
    resource = Column(String(50), nullable=False)
    action = Column(String(50), nullable=False)

    roles = relationship('Role', secondary='role_permissions', back_populates='permissions')

    def __repr__(self):
        return f'Permission {self.name}'