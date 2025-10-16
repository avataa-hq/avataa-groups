import asyncio
import functools
from logging import getLogger
from typing import TYPE_CHECKING, Callable

from base.base_accessor import BaseAccessor
from confluent_kafka import Consumer, KafkaException, TopicPartition, cimpl
from confluent_kafka.admin import TopicMetadata
from crud.crud_element import crud_element
from crud.crud_group import crud_group
from crud.crud_group_type import crud_group_type
from crud.group import GroupService
from schemas.schema_group import GroupSchema

from store.kafka.buffered_mo_worker import (
    AutoGroupSubscriber,
    BufferedMoWorker,
    BufferedTmoWorker,
    BufferedTprmWorker,
    TMOSubscriber,
    TPRMSubscriber,
)

from .inventory_services.utils import InventoryChangesHandler
from .kafka_models import (
    kafka_protobuf_message_action,
    kafka_protobuf_message_type,
)

if TYPE_CHECKING:
    from core.app import Application


class CKafkaConsumer(BaseAccessor):
    def __init__(
        self,
        app: "Application",
        token_callback: Callable[[None], tuple[str, float]],
        loop=None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(app, *args, **kwargs)
        self.__connected = False
        self.logger = getLogger("Conf_kafka Cons")
        self.token_callback = token_callback

        self.loop = loop or asyncio.get_running_loop()
        self._consumer: Consumer | None = None
        self._workers: dict | None = None

        self.task: asyncio.Task | None = None

        self.buffer_timeout: int = -1
        self.start_timeout = 5

    @property
    def consumer(self) -> Consumer:
        if not self._consumer:
            dump_set = {
                "bootstrap_servers",
                "group_id",
                "auto_offset_reset",
                "enable_auto_commit",
            }
            if self.app.config.kafka.secured:
                dump_set.update(
                    {
                        "sasl_mechanism",
                    }
                )
            consumer_config = self.app.config.kafka.model_dump(
                by_alias=True,
                exclude_none=True,
                include=dump_set,
            )
            consumer_config["error_cb"] = functools.partial(self._error_cb)
            consumer_config["oauth_cb"] = functools.partial(self.token_callback)
            self._consumer = Consumer(consumer_config)
        return self._consumer

    @staticmethod
    def _error_cb(e):
        print(e)

    async def connect(self, app: "Application") -> None:
        if not app.config.kafka.turn_on:
            return
        exist_group_scheme_first_time = True
        existed_groups = await self._get_existed_groups()
        try:
            while True:
                if app.store.group_scheme or not existed_groups:
                    break
                elif exist_group_scheme_first_time:
                    exist_group_scheme_first_time = False
                    self.logger.error(
                        "Can't run kafka accessor before dynamic model will be created."
                    )
                await asyncio.sleep(self.start_timeout)
            self._workers = self._create_workers()
            self.task = asyncio.create_task(
                self.__start_to_read_connect_to_kafka_topic()
            )
            self.buffer_timeout = 5

            self.logger.info(msg="started.")
        except (KeyboardInterrupt, asyncio.CancelledError):
            self.logger.info(msg="stopped.")
        except Exception as ex:
            self.logger.exception(ex)

    async def disconnect(self, app: "Application") -> None:
        if self._consumer:
            self.__connected = False
            self.consumer.close()
        if self.task:
            self.task.cancel()
            await self.task
        self.logger.info(msg="Disconnect.")

    def _on_assign(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        cons_id = consumer.memberid()
        for p in partitions:
            self.logger.info(
                f"Consumer {cons_id} assigned to the topic: {p.topic}, partition {p.partition}."
            )

    def _on_lost(self, consumer, partitions: list[TopicPartition]) -> None:
        cons_id = consumer.memberid()
        for p in partitions:
            self.logger.info(
                f"Consumer {cons_id} lost the topic: {p.topic}, partition {p.partition}."
            )

    def _on_revoke(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        self.consumer.commit()
        cons_id = consumer.memberid()
        self.logger.info(f"Consumer {cons_id} will be rebalanced.")

    async def __start_to_read_connect_to_kafka_topic(self) -> None:
        await self._check_topic_existence()
        self.consumer.subscribe(
            topics=[self.app.config.kafka.inventory_topic],
            on_assign=self._on_assign,
            on_lost=self._on_lost,
            on_revoke=self._on_revoke,
        )

        self.__connected = True

        while self.__connected:
            try:
                poll = functools.partial(self.consumer.poll, 3.0)
                msg = await self.loop.run_in_executor(None, poll)
                if msg is None:
                    continue
                handler_inst = InventoryChangesHandler(
                    kafka_msg=msg,
                    topic=self.app.config.kafka.inventory_topic,
                    workers=self._workers,
                )
                await handler_inst.process_the_message()
                self.consumer.commit(asynchronous=True, message=msg)
            except TypeError as ex:
                self.logger.error(msg=f"Kafka consumer: {type(ex)}: {ex}.")
            except (KeyboardInterrupt, asyncio.CancelledError):
                self.consumer.close()
                self.logger.info(msg="stopped.")
            except Exception as ex:
                self.logger.error(
                    msg=f"Kafka consumer error: {type(ex)}: {ex}."
                )

    async def _check_topic_existence(self) -> None:
        while True:
            try:
                # We should use poll to get keycloak token for authorization on broker
                # https://github.com/confluentinc/confluent-kafka-python/issues/1713
                self.consumer.poll(1)
                topics: dict[str, TopicMetadata] = self.consumer.list_topics(
                    timeout=5
                ).topics
                if self.app.config.kafka.inventory_topic in topics.keys():
                    self.logger.info(
                        f"Topic:{self.app.config.kafka.inventory_topic} discovered successfully."
                    )
                    break
                else:
                    self.logger.info(
                        f"Topic:{self.app.config.kafka.inventory_topic} not found. Waiting 60 seconds before retrying."
                    )
                    await asyncio.sleep(60)
            except KafkaException as ex:
                self.logger.exception("Kafka Exception on Start: %s", ex)
                await asyncio.sleep(60)
            except Exception as ex:
                self.logger.exception("Python Exception on Start: %s", ex)
                await asyncio.sleep(60)

    async def _get_existed_groups(self) -> list[GroupSchema]:
        try:
            async with self.app.database.session() as session:
                group_service = GroupService(
                    repo_group=crud_group,
                    repo_group_type=crud_group_type,
                    repo_element=crud_element,
                    session=session,
                    lifespan_app=self.app,
                )
                existed_groups: list[
                    GroupSchema
                ] = await group_service.get_all_groups()
                return existed_groups
        except Exception as ex:
            self.logger.exception(ex)

    def _exam_new_message(self, message: cimpl.Message) -> bool:
        result = False
        # MO:updated
        message_key = message.key().decode("utf-8")
        if message_key.find(":") == -1:
            result = True
            return result
        # MO, updated
        self.msg_class_name, self.msg_event = message_key.split(":")
        if self.msg_class_name not in kafka_protobuf_message_type.keys():
            result = True
            return result
        if (
            self.msg_class_name in {"PRM", "TPRM", "Process", "MO", "TMO"}
            and self.msg_event in kafka_protobuf_message_action.values()
        ):
            self.msg_object = kafka_protobuf_message_type[self.msg_class_name]()
            self.msg_object.ParseFromString(message.value())
            return result
        else:
            result = True
            return result

    def _create_workers(self) -> dict:
        auto_group_subscriber = AutoGroupSubscriber(app=self.app)
        tmo_subscriber = TMOSubscriber(app=self.app)
        tprm_subscriber = TPRMSubscriber(app=self.app)
        return {
            "TMO": BufferedTmoWorker(
                timeout_sec=self.app.config.buffered_mo.KAFKA_BUFFER_TIMEOUT_SEC,
                subscribers=tmo_subscriber,
            ),
            "TPRM": BufferedTprmWorker(
                timeout_sec=self.app.config.buffered_mo.KAFKA_BUFFER_TIMEOUT_SEC,
                subscribers=tprm_subscriber,
            ),
            "MO": BufferedMoWorker(
                timeout_sec=self.app.config.buffered_mo.KAFKA_BUFFER_TIMEOUT_SEC,
                subscribers=auto_group_subscriber,
            ),
        }
