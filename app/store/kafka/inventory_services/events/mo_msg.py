from logging import getLogger

logger = getLogger(__name__)


async def on_create_mo(msg, worker) -> None:
    logger.debug("on_create_mo %s", msg)
    worker.notify(message_type="MO", action="created", messages=[msg])


async def on_delete_mo(msg, worker) -> None:
    logger.debug("on_delete_mo %s", msg)
    worker.notify(message_type="MO", action="deleted", messages=[msg])


async def on_update_mo(msg, worker) -> None:
    logger.debug("on_update_mo %s", msg)
    worker.notify(message_type="MO", action="updated", messages=[msg])
