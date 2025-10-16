from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.app import Application


class BaseAccessor:
    """
    Base accessor is the base class for classes communicating with external services.
    Serves for connecting and gracefully disconnecting to external services when starting
    and stopping the application.
    """

    def __init__(self, app: "Application", *args, **kwargs):
        self.app = app
        self.logger = getLogger("accessor")
        app.on_startup.append(self.connect)
        app.on_shutdown.append(self.disconnect)

    async def connect(self, app: "Application"):
        return

    async def disconnect(self, app: "Application"):
        return
