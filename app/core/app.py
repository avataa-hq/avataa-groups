from store import Store, setup_store
from store.db.accessor import Database

from .config import Config, setup_config
from .logger import setup_logging


class Application:
    """
    Class used to get access to the configuration, database and external services
    """

    config: Config
    store: Store | None = None
    database: Database | None = None
    on_startup: list = []
    on_shutdown: list = []


app = Application()


def setup_app() -> Application:
    """
    Initial setup all necessary objects

    :return: Application object
    """
    app.config = setup_config()
    setup_logging(app)
    setup_store(app=app)

    return app
