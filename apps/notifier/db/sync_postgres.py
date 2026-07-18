from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common import get_settings
from models.notify import Base

config = get_settings()

engine_kwargs = {}

engine = create_engine(config.notify.notifier_bd.replace('+asyncpg', ''), **engine_kwargs)
SyncSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SyncSessionLocal()
    try:
        yield db
    finally:
        db.close()