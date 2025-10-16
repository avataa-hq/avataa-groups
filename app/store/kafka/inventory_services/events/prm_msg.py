from logging import getLogger

logger = getLogger(__name__)


async def on_create_prm(msg, worker) -> None:
    logger.debug("on_create_prm %s", msg)
    # worker.notify(message_type="PRM", action="created", messages=[msg])


async def on_delete_prm(msg, worker) -> None:
    logger.debug("on_delete_prm %s", msg)
    # worker.notify(message_type="PRM", action="updated", messages=[msg])


async def on_update_prm(msg, worker) -> None:
    logger.debug("on_update_prm %s", msg)
    # worker.notify(message_type="PRM", action="deleted", messages=[msg])
