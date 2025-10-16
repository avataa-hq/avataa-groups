from logging import getLogger

from store.kafka.msg_protocol import KafkaMSGProtocol

logger = getLogger(__name__)


async def on_create_tmo(msg, worker) -> None:
    logger.debug("on_create_tmo %s", msg)


async def on_delete_tmo(msg, worker) -> None:
    logger.debug("on_delete_tmo %s", msg)
    worker.notify(message_type="TMO", action="deleted", messages=[msg])


async def on_update_tmo(msg: KafkaMSGProtocol, worker) -> None:
    logger.debug("on_update_tmo %s", msg)
