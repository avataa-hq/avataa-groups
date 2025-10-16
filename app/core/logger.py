import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.app import Application


def setup_logging(app: "Application") -> None:
    """
    Initial setup for logging
    :param app: Application instance
    :return:
    """
    lvl = logging.CRITICAL
    match app.config.common.LOGGING:
        case 50:
            lvl = logging.CRITICAL
        case 40:
            lvl = logging.ERROR
        case 30:
            lvl = logging.WARNING
        case 20:
            lvl = logging.INFO
        case 10:
            lvl = logging.DEBUG
    if app.config.common.LOG_WITH_TIME:
        fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    else:
        fmt = "%(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(level=lvl, format=fmt)
    logging.critical(
        f"SET {logging.getLevelName(app.config.common.LOGGING)} LEVEL"
    )
