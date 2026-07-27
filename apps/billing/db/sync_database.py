from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.settings import get_settings

settings = get_settings()
sync_db_url = settings.postgres.db_url
sync_engine = create_engine(sync_db_url)
SyncSessionLocal = sessionmaker(sync_engine)
