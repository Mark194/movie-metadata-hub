import asyncio
import sys
from pathlib import Path
from logging.config import fileConfig

from sqlalchemy.ext.asyncio import create_async_engine
from alembic import context

# Добавляем путь к исходникам вашего приложения
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from db.postgres import Base
from common.settings import get_settings
from models.login_history import LoginHistory
from models.permission import Permission
from models.role import Role
from models.user import User

config = context.config
fileConfig(config.config_file_name)

target_metadata = Base.metadata

def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table":
        if name in target_metadata.tables:
            return True
        return False
    return True

def run_migrations_offline():
    settings = get_settings()
    context.configure(
        url=settings.postgres.async_db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata, include_object=include_object)
    with context.begin_transaction():
        context.run_migrations()

async def run_migrations_online():
    settings = get_settings()
    engine = create_async_engine(settings.postgres.async_db_url)

    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await engine.dispose()

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())