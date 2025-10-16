from logging import getLogger
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if TYPE_CHECKING:
    from core.app import Application


class Database:
    """
    Class realised logic for connect and disconnect to database.
    """

    def __init__(self, app: "Application"):
        self.app = app
        self.logger = getLogger("DB_Accessor")
        self._engine: AsyncEngine | None = None
        # self._db: declarative_base | None = None
        self.session: async_sessionmaker | None = None
        self.logger.debug("Database Accessor created.")

    async def connect(self, *_: list, **__: dict) -> None:
        echo_status = False
        if self.app.config.common.LOGGING < 19:
            echo_status = True
        self._engine = create_async_engine(
            self.app.config.db.url.unicode_string(),
            echo=echo_status,
            pool_pre_ping=True,
            connect_args={
                "server_settings": {
                    "application_name": "GROUP MS",
                    "search_path": self.app.config.db.schema_name,
                },
            },
        )
        self.session = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
        self.logger.debug(msg="Connected to DB.")

    async def disconnect(self, *_: list, **__: dict) -> None:
        if self.session:
            await self.session().close()
        if self._engine:
            await self._engine.dispose()
        self.logger.debug(msg="DB successfully disconnected.")
