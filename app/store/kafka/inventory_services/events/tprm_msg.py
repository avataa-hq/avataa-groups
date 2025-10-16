from logging import getLogger

logger = getLogger(__name__)


async def on_create_tprm(msg, worker) -> None:
    logger.debug("on_create_tprm %s", msg)
    worker.notify(message_type="TPRM", action="created", messages=[msg])


async def on_delete_tprm(msg, worker) -> None:
    logger.debug("on_delete_tprm %s", msg)
    worker.notify(message_type="TPRM", action="deleted", messages=[msg])


async def on_update_tprm(msg, worker) -> None:
    logger.debug("on_update_tprm %s", msg)
    worker.notify(message_type="TPRM", action="updated", messages=[msg])
