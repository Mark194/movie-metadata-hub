from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common import get_settings
from models import Base

config = get_settings()

engine_kwargs = {}

engine = create_engine(config.notify.notifier_bd, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Создание таблиц (при необходимости)
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()