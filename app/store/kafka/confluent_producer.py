import asyncio
import functools
from logging import getLogger
from typing import TYPE_CHECKING, Callable

from base.base_accessor import BaseAccessor
from confluent_kafka import Producer
from schemas.schema_group import GroupForKafka

import store.kafka.protobuf.group_pb2 as group
from store.kafka.msg_protocol import KafkaMSGProtocol

if TYPE_CHECKING:
    from core.app import Application


class CKafkaProducer(BaseAccessor):
    def __init__(
        self,
        app: "Application",
        token_callback: Callable[[None], tuple[str, float]],
        loop=None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(app, *args, **kwargs)
        self.group_topic = self.app.config.kafka.topic
        self.statistic_topic = self.app.config.kafka.group_data_changes_proto
        self.token_callback = token_callback
        self.logger = getLogger("Conf_kafka Prod")
        self._producer: Producer | None = None

        self.msg_size = 100

    @property
    def producer(self) -> Producer:
        if not self._producer:
            dump_set = {
                "bootstrap_servers",
            }
            if self.app.config.kafka.secured:
                dump_set.update(
                    {
                        "sasl_mechanism",
                    }
                )
            producer_config = self.app.config.kafka.model_dump(
                by_alias=True,
                exclude_none=True,
                include=dump_set,
            )
            # Debug Kafka
            # consumer_config["debug"] = "security,broker,protocol"
            producer_config["error_cb"] = functools.partial(self._error_cb)
            producer_config["oauth_cb"] = functools.partial(self.token_callback)
            self._producer = Producer(producer_config)
        return self._producer

    @staticmethod
    def _error_cb(ex: Exception) -> None:
        print(ex)

    async def disconnect(self, app: "Application") -> None:
        if self._producer:
            self._producer.flush()
        self.logger.info("Kafka producer closed.")

    async def connect(self, app: "Application"):
        self._producer = self.producer

    async def send_message_about_group_entity(
        self, data: GroupForKafka, action: str
    ) -> None:
        self.logger.info(
            msg=f"Send message to Kafka with {action=} and {data=}"
        )
        chunks = [
            data.entity_ids[i : i + self.msg_size]
            for i in range(0, len(data.entity_ids), self.msg_size)
        ]
        message = group.GROUP()
        message.group_name = data.group_name
        message.group_type = data.group_type
        message.tmo_id = data.tmo_id
        try:
            if chunks:
                for chunk in chunks:
                    message.entity_id.extend(chunk)
                    self._producer.produce(
                        topic=self.group_topic,
                        key=bytes(action, "utf-8"),
                        value=message.SerializeToString(),
                        on_delivery=self._delivery_report,
                    )
            else:
                self._producer.produce(
                    topic=self.group_topic,
                    key=bytes(action, "utf-8"),
                    value=message.SerializeToString(),
                    on_delivery=self._delivery_report,
                )
            self._producer.flush()

        except asyncio.CancelledError:
            self._producer.flush()
            self.logger.warning(msg="producer stopped.")
        except AttributeError as ex:
            self.logger.warning(
                "Error occurred in method 'send_message_about_group_entity'."
            )
            self.logger.exception(ex)
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    async def send_message_about_group_statistic(self, message, action: str):
        self.logger.info("Send statistic to Kafka:\n%s", message)
        try:
            self._producer.produce(
                topic=self.statistic_topic,
                key=bytes(action, "utf-8"),
                value=message.SerializeToString(),
                on_delivery=self._delivery_report,
            )
            self._producer.flush()

        except asyncio.CancelledError:
            self._producer.flush()
            self.logger.warning(msg="producer stopped.")
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    def _delivery_report(self, err, msg: KafkaMSGProtocol) -> None:
        """
        Reports the failure or success of a message delivery.
        Args:
            err (KafkaError): The error that occurred on None on success.
            msg (Message): The message that was produced or failed.
        """

        if err is not None:
            self.logger.warning(
                "Delivery failed for User record %s: %s", msg.key(), err
            )
            return
        self.logger.info(
            "User record %s successfully produced to %s [%s] at offset %d",
            msg.key(),
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
