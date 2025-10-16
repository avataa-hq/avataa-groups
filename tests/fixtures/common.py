from typing import AsyncGenerator, AsyncIterator

import pytest_asyncio
from fastapi import FastAPI

from tests.config import TestsConfig
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool
from store import Store
from store.db.base import Base
from testcontainers.postgres import PostgresContainer

# @pytest_asyncio.fixture(scope="session")
# def server():
#     app = setup_app()
#     app.on_startup.clear()
#     app.on_shutdown.clear()
#     app.store.redis = AsyncMock()
#     app.store.api = AsyncMock()
#     app.store.grpc = AsyncMock()
#
#     return app


@pytest_asyncio.fixture(scope="session")
def store(server) -> Store:
    return server.store


# @pytest_asyncio.fixture(scope="session")
# def config(server) -> Config:
#     return server.config


# def set_sqlite_pragma(dbapi_connection, connection_record):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA foreign_keys=ON")
#     cursor.close()


# @pytest_asyncio.fixture(autouse=True)
# async def setup_database(test_engine: AsyncEngine) -> AsyncGenerator:
#     async with test_engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all)
#         await conn.run_sync(Base.metadata.create_all)
#     yield
#     async with test_engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="session")
def db_url():
    tests = TestsConfig()
    if tests.run_container_postgres_local:
        with PostgresContainer(
            username=tests.user,
            password=tests.db_pass,
            dbname="test_db",
            driver="asyncpg",
        ) as postgres:
            yield postgres.get_connection_url()
    else:
        db_url = tests.url.unicode_string()
        yield db_url


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_engine(db_url) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(
        url=db_url,
        poolclass=StaticPool,
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def async_session(test_engine: AsyncEngine) -> AsyncGenerator:
    async_session = async_sessionmaker(
        test_engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as s:
        yield s


@pytest_asyncio.fixture(scope="session")
async def client() -> AsyncIterator[AsyncClient]:
    app = FastAPI()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://127.0.0.2",
    ) as client:
        yield client
