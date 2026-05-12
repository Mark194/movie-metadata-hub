from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()

_engine = None
_async_session_maker = None


async def init_postgres(database_url: str):
    global _engine, _async_session_maker
    _engine = create_async_engine(database_url, echo=True)
    _async_session_maker = async_sessionmaker(_engine, expire_on_commit=False)


async def get_postgres() -> AsyncSession:
    if _async_session_maker is None:
        raise RuntimeError("PostgreSQL not initialized")
    async with _async_session_maker() as session:
        yield session


async def close_postgres():
    """Закрывает соединения с БД."""
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None
