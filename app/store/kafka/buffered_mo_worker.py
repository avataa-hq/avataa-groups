import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Task
from math import ceil
from time import time
from typing import TYPE_CHECKING, Callable, Iterator, Literal, Sequence

from api.api_v1.endpoints.utils.elements_utils import (
    format_data_from_model_to_kafka_message_for_statistic,
)
from crud.crud_element import crud_element
from crud.crud_group import crud_group
from crud.crud_group_template import crud_group_template
from crud.crud_group_type import crud_group_type
from crud.group import GroupService
from crud.group_template import GroupTemplateService
from models.model_element import ElementModel
from models.model_group import GroupModel
from models.model_group_template import GroupTemplateModel
from pydantic import BaseModel
from schemas.schema_element import (
    ElementReadyToDB,
    ElementResponse,
    ElementSchema,
)
from schemas.schema_group import GroupBase, GroupForKafka, GroupSchema
from schemas.schema_group_template import GroupTemplateSchema

from store.kafka.kafka_models import (
    T,
    kafka_protobuf_message_action,
    kafka_protobuf_message_type,
)
from store.kafka.protobuf import statistic_pb2
from store.kafka.protobuf.inventory_instances_pb2 import PRM

if TYPE_CHECKING:
    from core.app import Application


class BufferedMoWorkerSubscriber(ABC):
    @abstractmethod
    async def update(self, mo_ids: list[int]):
        # list will not be empty
        raise NotImplementedError(
            "BufferedMoWorkerSubscriber is not implemented"
        )


class BufferedTmoWorkerSubscriber(ABC):
    @abstractmethod
    async def update_tmo(self, tmo_ids: list[int]):
        # list will not be empty
        raise NotImplementedError(
            "BufferedTMoWorkerSubscriber is not implemented"
        )

    async def delete_tmo(self, tmo_ids: list[int]):
        # list will not be empty
        raise NotImplementedError(
            "BufferedTMoWorkerSubscriber is not implemented"
        )


class BufferedTprmWorkerSubscriber(ABC):
    @abstractmethod
    async def update_tprms(self, tprm_ids: list[int]):
        # list will not be empty
        raise NotImplementedError(
            "BufferedTprmWorkerSubscriber is not implemented"
        )

    async def delete_tprms(self, tprm_ids: list[int]):
        # list will not be empty
        raise NotImplementedError(
            "BufferedTprmWorkerSubscriber is not implemented"
        )

    async def create_tprms(self, tprm_ids: list[int]):
        # list will not be empty
        raise NotImplementedError(
            "BufferedTprmWorkerSubscriber is not implemented"
        )


class BufferedMoWorker:
    FILTER_MSG_CLASS_NAME = {"MO", "PRM"}
    FILTER_MSG_ACTION = {"created", "updated", "deleted"}

    def __init__(
        self,
        timeout_sec: int,
        subscribers: Sequence[BufferedMoWorkerSubscriber]
        | BufferedMoWorkerSubscriber
        | None = None,
    ):
        self.timeout_sec = timeout_sec
        self.__mo_id_buffer = set()
        self._subscribers = []
        self._periodical_task_instance: Task | None = None
        self.logger = logging.getLogger("Buffered MO Worker")
        self._initial_subscribers(subscribers=subscribers)

    def _initial_subscribers(
        self,
        subscribers: Sequence[BufferedMoWorkerSubscriber]
        | BufferedMoWorkerSubscriber,
    ):
        if isinstance(subscribers, BufferedMoWorkerSubscriber):
            self.subscribe(subscribers)
        elif isinstance(subscribers, Sequence):
            for subscriber in subscribers:
                if isinstance(subscriber, BufferedMoWorkerSubscriber):
                    self.subscribe(subscriber)

    def _message_filter(
        self,
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        action: Literal[*kafka_protobuf_message_action.keys()],  # noqa
        messages: list[T],
    ) -> Iterator[T]:
        if message_type not in self.FILTER_MSG_CLASS_NAME:
            return
        if action not in self.FILTER_MSG_ACTION:
            return
        if isinstance(messages, list):
            yield from messages
        else:
            yield from messages.objects

    @staticmethod
    def _convert_message(
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        # action: Literal[*kafka_protobuf_message_action.keys()],
        message: T,
    ) -> int | None:
        if hasattr(message, "mo_id"):
            return getattr(message, "mo_id")
        elif message_type == "MO" and hasattr(message, "id"):
            return getattr(message, "id")

    def subscribe(self, subscriber: BufferedMoWorkerSubscriber):
        if subscriber not in self._subscribers:
            self._subscribers.append(subscriber)
        if (
            len(self._subscribers) == 1
            and self._periodical_task_instance is None
        ):
            self._create_periodical_task()

    def unsubscribe(self, subscriber: BufferedMoWorkerSubscriber):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
        if not self._subscribers and self._periodical_task_instance is not None:
            self._delete_periodical_task()

    async def _task(self):
        if not self.__mo_id_buffer:
            return
        subscribers = self._subscribers.copy()
        mo_ids = self.__mo_id_buffer.copy()
        self.__mo_id_buffer = set()
        self.logger.debug(
            "Invoke update task for %d subscribers", len(subscribers)
        )
        for subscriber in subscribers:
            await subscriber.update(mo_ids=list(mo_ids))

    def __del__(self):
        self.unsubscribe(self._subscribers)

    @staticmethod
    async def _periodical_task(period_sec: int, task: Callable):
        try:
            while True:
                loop_start = time()
                await task()
                loop_end = time()
                delta = loop_end - loop_start
                if delta < period_sec:
                    sleep_time = ceil(period_sec - delta)
                    await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            await asyncio.wait_for(task(), 1)
            print("Cancelled update task for Buffered MO Worker")
        except KeyboardInterrupt:
            await asyncio.wait_for(task(), 1)
            print("Task cancelled by user")
        except Exception as ex:
            print(f"Error in task: {type(ex)}: {ex}")
            raise ex

    def _create_periodical_task(self):
        try:
            if not self._periodical_task_instance:
                self._periodical_task_instance = asyncio.create_task(
                    self._periodical_task(
                        period_sec=self.timeout_sec, task=self._task
                    )
                )
            return self._periodical_task_instance
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.logger.warning("Cancelled Error")
            self._periodical_task_instance.cancel()
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    def _delete_periodical_task(self):
        if self._periodical_task_instance:
            if not self._periodical_task_instance.cancelled():
                self._periodical_task_instance.cancel()
            self._periodical_task_instance = None

    def notify(
        self,
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        action: Literal[*kafka_protobuf_message_action.keys()],  # noqa
        messages: list[T],
    ):
        if not self._subscribers:
            return
        for message in self._message_filter(
            message_type=message_type, action=action, messages=messages
        ):
            # mo_id = self._convert_message(
            #     message=message, message_type=message_type
            # )
            for obj in message.get("objects", None):  # type: dict[str, str]
                mo_id = obj.get("id")
                if mo_id is None:
                    continue
                self.__mo_id_buffer.add(mo_id)


class BufferedTmoWorker:
    FILTER_MSG_CLASS_NAME = {"TMO"}
    FILTER_MSG_ACTION = {"updated", "deleted"}

    def __init__(
        self,
        timeout_sec: int,
        subscribers: Sequence[BufferedTmoWorkerSubscriber]
        | BufferedTmoWorkerSubscriber
        | None = None,
    ):
        self.timeout_sec = timeout_sec
        self._tmo_id_buffer_to_delete = set()
        self._tmo_id_buffer_to_update = set()
        self._subscribers = []
        self._periodical_task_instance: Task | None = None
        self.logger = logging.getLogger("Buffered TMO Worker")
        self._initial_subscribers(subscribers=subscribers)

    def _initial_subscribers(
        self,
        subscribers: Sequence[BufferedTmoWorkerSubscriber]
        | BufferedTmoWorkerSubscriber,
    ):
        if isinstance(subscribers, BufferedTmoWorkerSubscriber):
            self.subscribe(subscribers)
        elif isinstance(subscribers, Sequence):
            for subscriber in subscribers:
                if isinstance(subscriber, BufferedTmoWorkerSubscriber):
                    self.subscribe(subscriber)

    def _message_filter(
        self,
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        action: Literal[*kafka_protobuf_message_action.keys()],  # noqa
        messages: list[T],
    ) -> Iterator[T]:
        # Отфильтровываем лишние сообщения
        if message_type not in self.FILTER_MSG_CLASS_NAME:
            return
        if action not in self.FILTER_MSG_ACTION:
            return
        if isinstance(messages, list):
            yield from messages
        else:
            yield from messages.objects

    @staticmethod
    def _convert_message(
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        # action: Literal[*kafka_protobuf_message_action.keys()],
        message: T,
    ) -> int | None:
        if hasattr(message, "id"):
            return getattr(message, "id")

    def subscribe(self, subscriber: BufferedMoWorkerSubscriber):
        if subscriber not in self._subscribers:
            self._subscribers.append(subscriber)
        if (
            len(self._subscribers) == 1
            and self._periodical_task_instance is None
        ):
            self._create_periodical_task()

    def unsubscribe(self, subscriber: BufferedMoWorkerSubscriber):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
        if not self._subscribers and self._periodical_task_instance is not None:
            self._delete_periodical_task()

    async def _task(self):
        if (
            not self._tmo_id_buffer_to_update
            and not self._tmo_id_buffer_to_delete
        ):
            return
        subscribers = self._subscribers.copy()
        # tmo_ids_to_update = self._tmo_id_buffer_to_update.copy()
        tmo_ids_to_delete = self._tmo_id_buffer_to_delete.copy()
        self._tmo_id_buffer_to_update = self._tmo_id_buffer_to_delete = set()
        self.logger.debug(
            "Invoke update task for %d subscribers", len(subscribers)
        )
        for subscriber in subscribers:
            if tmo_ids_to_delete:
                await subscriber.delete_tmo(tmo_ids=list(tmo_ids_to_delete))
            # update TMO logic
            # else:
            #     await subscriber.update_tmo(tmo_ids=list(tmo_ids_to_update))

    def __del__(self):
        self.unsubscribe(self._subscribers)

    @staticmethod
    async def _periodical_task(period_sec: int, task: Callable):
        try:
            while True:
                loop_start = time()
                await task()
                loop_end = time()
                delta = loop_end - loop_start
                if delta < period_sec:
                    sleep_time = ceil(period_sec - delta)
                    await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            await asyncio.wait_for(task(), 1)
            print("Cancelled update task for Buffered TMO Worker")
        except KeyboardInterrupt:
            await asyncio.wait_for(task(), 1)
            print("Task cancelled by user")
        except Exception as ex:
            print(f"Error in task: {type(ex)}: {ex}")
            raise ex

    def _create_periodical_task(self):
        try:
            if not self._periodical_task_instance:
                self._periodical_task_instance = asyncio.create_task(
                    self._periodical_task(
                        period_sec=self.timeout_sec, task=self._task
                    )
                )
            return self._periodical_task_instance
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.logger.warning("Cancelled Error")
            self._periodical_task_instance.cancel()
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    def _delete_periodical_task(self):
        if self._periodical_task_instance:
            if not self._periodical_task_instance.cancelled():
                self._periodical_task_instance.cancel()
            self._periodical_task_instance = None

    def notify(
        self,
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        action: Literal[*kafka_protobuf_message_action.keys()],  # noqa
        messages: list[T],
    ):
        if not self._subscribers:
            return
        for message in self._message_filter(
            message_type=message_type, action=action, messages=messages
        ):
            tmo_id = self._convert_message(
                message=message, message_type=message_type
            )
            if tmo_id is None:
                continue
            if action == "deleted":
                self._tmo_id_buffer_to_delete.add(tmo_id)
            # else:
            #     self._tmo_id_buffer_to_update.add(tmo_id)


class BufferedTprmWorker:
    FILTER_MSG_CLASS_NAME = {"TRM"}
    FILTER_MSG_ACTION = {"created", "updated", "deleted"}

    def __init__(
        self,
        timeout_sec: int,
        subscribers: Sequence[BufferedTprmWorkerSubscriber]
        | BufferedTprmWorkerSubscriber
        | None = None,
    ):
        self.timeout_sec = timeout_sec
        self._tprm_id_buffer_to_delete = set()
        self._tprm_id_buffer_to_update = set()
        self._tprm_id_buffer_to_create = set()
        self._subscribers = []
        self._periodical_task_instance: Task | None = None
        self.logger = logging.getLogger("Buffered TPRM Worker")
        self._initial_subscribers(subscribers=subscribers)

    def _initial_subscribers(
        self,
        subscribers: Sequence[BufferedTprmWorkerSubscriber]
        | BufferedTprmWorkerSubscriber,
    ):
        if isinstance(subscribers, BufferedTprmWorkerSubscriber):
            self.subscribe(subscribers)
        elif isinstance(subscribers, Sequence):
            for subscriber in subscribers:
                if isinstance(subscriber, BufferedTprmWorkerSubscriber):
                    self.subscribe(subscriber)

    def _message_filter(
        self,
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        action: Literal[*kafka_protobuf_message_action.keys()],  # noqa
        messages: list[T],
    ) -> Iterator[T]:
        # Отфильтровываем лишние сообщения
        if message_type not in self.FILTER_MSG_CLASS_NAME:
            return
        if action not in self.FILTER_MSG_ACTION:
            return
        if isinstance(messages, list):
            yield from messages
        else:
            yield from messages.objects

    @staticmethod
    def _convert_message(
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        # action: Literal[*kafka_protobuf_message_action.keys()],
        message: T,
    ) -> int | None:
        if hasattr(message, "id"):
            return getattr(message, "id")

    def subscribe(self, subscriber: BufferedMoWorkerSubscriber):
        if subscriber not in self._subscribers:
            self._subscribers.append(subscriber)
        if (
            len(self._subscribers) == 1
            and self._periodical_task_instance is None
        ):
            self._create_periodical_task()

    def unsubscribe(self, subscriber: BufferedMoWorkerSubscriber):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
        if not self._subscribers and self._periodical_task_instance is not None:
            self._delete_periodical_task()

    async def _task(self):
        if (
            not self._tprm_id_buffer_to_update
            and not self._tprm_id_buffer_to_delete
            and not self._tprm_id_buffer_to_create
        ):
            return
        subscribers = self._subscribers.copy()
        tprm_ids_to_delete = self._tprm_id_buffer_to_delete.copy()
        tprm_ids_to_create = self._tprm_id_buffer_to_create.copy()
        tprm_ids_to_update = self._tprm_id_buffer_to_update.copy()
        self._tprm_id_buffer_to_update = self._tprm_id_buffer_to_delete = (
            self._tprm_id_buffer_to_create
        ) = set()
        self.logger.debug(
            "Invoke update task for %d subscribers", len(subscribers)
        )
        for subscriber in subscribers:
            if self._tprm_id_buffer_to_create:
                await subscriber.create_tprm_ids(
                    tmo_ids=list(tprm_ids_to_create)
                )
            if self._tprm_id_buffer_to_delete:
                await subscriber.delete_tprm_ids(
                    tmo_ids=list(tprm_ids_to_delete)
                )
            if self._tprm_id_buffer_to_update:
                await subscriber.update_tprm_ids(
                    tmo_ids=list(tprm_ids_to_update)
                )

    def __del__(self):
        self.unsubscribe(self._subscribers)

    @staticmethod
    async def _periodical_task(period_sec: int, task: Callable):
        try:
            while True:
                loop_start = time()
                await task()
                loop_end = time()
                delta = loop_end - loop_start
                if delta < period_sec:
                    sleep_time = ceil(period_sec - delta)
                    await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            await asyncio.wait_for(task(), 1)
            print("Cancelled update task for Buffered TPRM Worker")
        except KeyboardInterrupt:
            await asyncio.wait_for(task(), 1)
            print("Task cancelled by user")
        except Exception as ex:
            print(f"Error in task: {type(ex)}: {ex}")
            raise ex

    def _create_periodical_task(self):
        try:
            if not self._periodical_task_instance:
                self._periodical_task_instance = asyncio.create_task(
                    self._periodical_task(
                        period_sec=self.timeout_sec, task=self._task
                    )
                )
            return self._periodical_task_instance
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.logger.warning("Cancelled Error")
            self._periodical_task_instance.cancel()
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    def _delete_periodical_task(self):
        if self._periodical_task_instance:
            if not self._periodical_task_instance.cancelled():
                self._periodical_task_instance.cancel()
            self._periodical_task_instance = None

    def notify(
        self,
        message_type: Literal[*kafka_protobuf_message_type.keys()],  # noqa
        action: Literal[*kafka_protobuf_message_action.keys()],  # noqa
        messages: list[T],
    ):
        if not self._subscribers:
            return
        for message in self._message_filter(
            message_type=message_type, action=action, messages=messages
        ):
            tprm_id = self._convert_message(
                message=message, message_type=message_type
            )
            if tprm_id is None:
                continue
            if action == "created":
                self._tprm_id_buffer_to_create.add(tprm_id)
            elif action == "deleted":
                self._tprm_id_buffer_to_delete.add(tprm_id)
            elif action == "updated":
                self._tprm_id_buffer_to_update.add(tprm_id)


class TestSubscriber(BufferedMoWorkerSubscriber):
    async def update(self, mo_ids: list[int]):
        print("subscriber mo_ids", mo_ids)


class AutoGroupSubscriber(BufferedMoWorkerSubscriber):
    def __init__(self, app: "Application"):
        self.app = app
        self.logger = logging.getLogger("Auto Group Subscriber")

    async def update(self, mo_ids: list[int]):
        self.logger.debug("Update auto group.")
        try:
            async with self.app.database.session() as session:
                list_of_group_template: list[
                    GroupTemplateModel
                ] = await crud_group_template.get_all_group_template(
                    session=session
                )
                list_groups = await crud_group.get_group_schema_by_element_id(
                    session=session, obj_in=mo_ids
                )
            self.logger.debug(
                "Total auto group template: %d", len(list_of_group_template)
            )
            for group_template in list_of_group_template:  # type: GroupTemplateModel
                await self.update_auto_group(group_template.to_schema())
            for group in list_groups:
                await self._update_group(existed_group=group)
        except asyncio.CancelledError:
            self.logger.warning("Buffered MO Worker stopped.")
        except Exception as ex:
            self.logger.exception(ex)

    async def update_auto_group(
        self, group_template: GroupTemplateSchema
    ) -> None:
        existed_groups: list[GroupSchema] = []
        default_group_name = f"auto_{group_template.name}_"
        if group_template.identical:
            try:
                search_data: list = (
                    await self.app.store.grpc.get_processes_group_from_search(
                        group_template
                    )
                )
            except ValueError:
                search_data = []
            for item in search_data:  # type: ResponseProcessesGroups
                temp_group_name = default_group_name + "_".join(
                    [gr.grouping_value for gr in item.item.group]
                )
                if group_template.column_filters:
                    filter_to_name = [
                        i["operator"] + "_" + str(i["value"])
                        for c in group_template.column_filters
                        for i in c["filters"]
                    ]
                    temp_group_name += "_" + "_".join(filter_to_name)
                self.logger.debug(
                    "Check qnt. Search MS has: %d. Template min: %d",
                    item.item.quantity,
                    group_template.min_qnt,
                )
                if item.item.quantity >= group_template.min_qnt:
                    # Check if group exists
                    async with self.app.database.session() as session:
                        group: GroupModel = await crud_group.get_group(
                            session=session, data=temp_group_name
                        )
                        if not group:
                            column_filter = [
                                {
                                    "filters": [
                                        {
                                            "operator": "equals",
                                            "value": gr.grouping_value,
                                        }
                                    ],
                                    "columnName": gr.grouped_by,
                                    "rule": "and",
                                }
                                for gr in item.item.group
                            ]
                            self.logger.debug(
                                "Create group with filter: %s",
                                column_filter,
                            )
                            new_group: GroupBase = GroupBase(
                                group_name=temp_group_name,
                                group_type_id=group_template.group_type_id_for_template,
                                tmo_id=group_template.tmo_id,
                                column_filters=column_filter,
                                ranges_object=group_template.ranges_object,
                                is_aggregate=False,
                                min_qnt=group_template.min_qnt,
                                group_template_id=group_template.id,
                            )
                            # Create group
                            group: GroupSchema = (
                                await crud_group.create_groups_schema(
                                    session=session,
                                    obj_in=[new_group],
                                )
                            )[0]
                    existed_groups.append(group)
                    self.logger.debug("Group existed and add new elements")
        elif group_template.column_filters:
            self.logger.debug(
                "Create group with filter: %s",
                group_template.column_filters,
            )
            chunk = [
                i["operator"] + "_" + str(i["value"])
                for c in group_template.column_filters
                for i in c["filters"]
            ]
            temp_group_name = default_group_name + "_".join(chunk)
            async with self.app.database.session() as session:
                temp_group: GroupSchema = await crud_group.get_group_schema(
                    session=session, data=temp_group_name
                )
                if not temp_group:
                    new_group: GroupBase = GroupBase(
                        group_name=temp_group_name,
                        group_type_id=group_template.group_type_id_for_template,
                        tmo_id=group_template.tmo_id,
                        column_filters=group_template.column_filters,
                        ranges_object=group_template.ranges_object,
                        is_aggregate=False,
                        min_qnt=group_template.min_qnt,
                        group_template_id=group_template.id,
                    )
                    # Create group
                    temp_group: GroupSchema = (
                        await crud_group.create_groups_schema(
                            session=session,
                            obj_in=[new_group],
                        )
                    )[0]
                existed_groups.append(temp_group)
        else:
            self.logger.warning(group_template)
            raise ValueError("Not enough data to create an auto group")
        self.logger.debug(
            "Total group for template: %s: %d",
            group_template.name,
            len(existed_groups),
        )
        for gr in existed_groups:  # type: GroupSchema
            if not self.app.store.group_scheme.get(str(gr.tmo_id), None):
                self.logger.info(
                    "Can't find correct group scheme. Rebuild group scheme."
                )
                await self.app.store.grpc.create_dynamic_statistic_model(
                    tmo_id=gr.tmo_id
                )
            await self._update_group(existed_group=gr)

    async def _update_group(self, existed_group: GroupSchema):
        # Add elements to group
        data_for_statistic: tuple[
            list[BaseModel], bool
        ] = await self.app.store.grpc.get_severity_processes(
            group_schema=existed_group
        )
        data_input_elements, is_valid = data_for_statistic
        list_input_entity_id: list[int] = [
            el.MO.id for el in data_input_elements
        ]
        if existed_group.is_valid != is_valid:
            async with self.app.database.session() as session:
                await crud_group.update_valid_schema(
                    session=session,
                    obj_in=existed_group,
                    is_valid=bool(is_valid),
                )
        # Get elements from input group
        async with self.app.database.session() as session:
            existed_elements_in_group: Sequence[
                ElementSchema
            ] = await crud_element.select_by_group_id_schema_with_update(
                session=session, group_id=existed_group.id
            )

            # Remove elements from input if they existed in current group
            if existed_elements_in_group:
                filtered_elements: set[int] = set(list_input_entity_id) - set(
                    el.entity_id for el in existed_elements_in_group
                )
                list_input_entity_id = list(filtered_elements)

            input_elements_ready_to_db: list[ElementReadyToDB] = []
            for element in list_input_entity_id:
                data = {"entity_id": element, "group_id": existed_group.id}
                input_elements_ready_to_db.append(ElementReadyToDB(**data))

            if (
                len(input_elements_ready_to_db) + len(existed_elements_in_group)
                > existed_group.min_qnt
            ):
                self.logger.info(
                    "Add %d elements to group %s",
                    len(input_elements_ready_to_db),
                    existed_group.group_name,
                )
                # Save entity id in database
                # new_element = []
                if input_elements_ready_to_db:
                    new_element: Sequence[
                        ElementModel
                    ] = await crud_element.create_element(
                        session=session, obj_in=input_elements_ready_to_db
                    )
                    response_new_element: list[ElementResponse] = []
                    for el in new_element:
                        response_new_element.append(
                            ElementResponse(
                                entity_id=el.entity_id,
                                group_id=el.group_id,
                                id=el.id,
                                description=bool(is_valid),
                            )
                        )
                    await self.app.store.redis.set_statistic_by_schema(
                        current_group=existed_group, data=data_input_elements
                    )
                    # Send message to Kafka about add element
                    group_for_kafka = GroupForKafka(
                        **{
                            "group_name": existed_group.group_name,
                            "entity_ids": [
                                el.MO.id for el in data_input_elements
                            ],
                            "group_type": existed_group.group_type.name,
                            "tmo_id": existed_group.tmo_id,
                        }
                    )
                    await self.app.store.kafka_prod.send_message_about_group_entity(
                        data=group_for_kafka, action="group:add"
                    )

                    statistic: BaseModel = (
                        await self.app.store.redis.get_statistic(existed_group)
                    )
                    kafka_statistic_format = (
                        format_data_from_model_to_kafka_message_for_statistic(
                            statistic=statistic,
                            group_type=existed_group.group_type.name,
                        )
                    )
                    await self.app.store.kafka_prod.send_message_about_group_statistic(
                        message=statistic_pb2.Statistic(
                            **kafka_statistic_format
                        ),
                        action="group_statistic:update",
                    )


class TMOSubscriber(BufferedTmoWorkerSubscriber):
    def __init__(self, app: "Application"):
        self.app = app
        self.logger = logging.getLogger("TMO Subscriber")

    async def update_tmo(self, tmo_ids: list[int]):
        self.logger.warning("TMO %s was updated", tmo_ids)

    async def delete_tmo(self, tmo_ids: list[int]):
        self.logger.warning("TMO %s was deleted", tmo_ids)
        async with self.app.database.session() as session:
            group_service = GroupService(
                repo_group=crud_group,
                repo_group_type=crud_group_type,
                repo_element=crud_element,
                session=session,
                lifespan_app=self.app,
            )
            group_template_service = GroupTemplateService(
                repo_group_template=crud_group_template,
                session=session,
                lifespan_app=self.app,
            )

            list_group_names_to_delete = (
                await group_service.get_group_names_by_tmo_id(tmo_ids=tmo_ids)
            )
            await group_service.remove_many_by_names_by_schema(
                group_names=list_group_names_to_delete
            )
            # await group_service.remove_many_by_tmo(tmo_ids=tmo_ids)
            self.logger.warning("Deleted groups for tmos: %s", tmo_ids)
            await group_template_service.remove_group_template_by_tmo_ids(
                tmo_ids=tmo_ids
            )
            self.logger.warning("Deleted group template for this tmo")


class TPRMSubscriber(BufferedTprmWorkerSubscriber):
    def __init__(self, app: "Application"):
        self.app = app
        self.logger = logging.getLogger("TPRM Subscriber")

    def update_tprms(self, tprm_ids: list[int]):
        pass

    def delete_tprms(self, tprm_ids: list[int]):
        pass

    def create_tprms(self, tprm_ids: list[int]):
        pass


async def main():
    subscriber = TestSubscriber()
    worker = BufferedMoWorker(timeout_sec=15, subscribers=subscriber)
    for i in range(20):
        print("main", i)
        worker.notify(
            message_type="PRM",
            action="created",
            messages=[PRM(id=1, value="1", tprm_id=1, mo_id=i, version=1)],
        )
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
