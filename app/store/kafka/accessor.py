# import asyncio
# import tracemalloc
# from datetime import datetime
#
# import psutil
#
# from logging import getLogger
# from typing import TYPE_CHECKING
#
# import aiokafka.errors
# from aiokafka import TopicPartition, ConsumerRecord
# from aiokafka.admin import AIOKafkaAdminClient, NewTopic
# from aiokafka.consumer import AIOKafkaConsumer
# from aiokafka.producer import AIOKafkaProducer
# from pydantic import BaseModel
#
# import store.kafka.protobuf.group_pb2 as group
# from base.base_accessor import BaseAccessor
# from crud import crud_group_type
# from crud.crud_element import crud_element
# from crud.crud_group import crud_group
# from crud.element import ElementService
# from crud.group import GroupService
# from models.model_group import GroupModel
# from schemas.schema_element import ElementReadyToDB
# from schemas.schema_group import GroupForKafka, GroupSchema
# from .CustomTokenProvider import AsyncCustomTokenProvider
# from .buffered_mo_worker import (
#     BufferedMoWorker,
#     AutoGroupSubscriber,
#     TMOSubscriber,
#     BufferedTmoWorker,
#     TPRMSubscriber,
#     BufferedTprmWorker,
# )
# from .kafka_models import (
#     T,
#     KafkaType,
#     KafkaConfigType,
#     kafka_protobuf_message_action,
#     kafka_protobuf_message_type,
# )
#
# if TYPE_CHECKING:
#     from core.app import Application
#
# IGNORE_PATHS = [
#     "tracemalloc.py",
# ]
#
#
# class KafkaAccessor(BaseAccessor):
#     def __init__(self, app: "Application", *args, **kwargs):
#         super().__init__(app, *args, **kwargs)
#         self.logger = getLogger("Kafka_Accessor")
#         self.kafka_producer: AIOKafkaProducer | None = None
#         self.kafka_consumer: AIOKafkaConsumer | None = None
#         self.kafka_client: AIOKafkaAdminClient | None = None
#         self.topic_to_read: list[TopicPartition] = []
#
#         self.token_provider: AsyncCustomTokenProvider | None = None
#         self.task_update_token_provider: asyncio.Task | None = None
#         self.task_consumer: asyncio.Task | None = None
#
#         self.task_update_buffer: asyncio.Task | None = None
#         self.buffer_timeout: int = -1
#         self.buffer: list[tuple[list[int], str]] = []
#         self.msg_object: T | None = None
#
#         self.buffered_mo_worker: BufferedMoWorker | None = None
#         self.buffered_tmo_worker: BufferedTmoWorker | None = None
#         self.buffered_tprm_worker: BufferedTprmWorker | None = None
#
#         self.element_service: ElementService | None = None
#         self.group_service: GroupService | None = None
#
#         self.start_timeout = 5
#         self.msg_size = 5000
#
#     async def connect(self, app: "Application"):
#         exist_group_scheme_first_time = True
#         # asyncio.create_task(self.log_memory_usage())
#         try:
#             async with self.app.database.session() as session:
#                 group_service = GroupService(
#                     repo_group=crud_group,
#                     repo_group_type=crud_group_type,
#                     repo_element=crud_element,
#                     session=session,
#                     lifespan_app=self.app,
#                 )
#                 # element_service = ElementService(
#                 #     repo_group=crud_group,
#                 #     repo_element=crud_element,
#                 #     session=session,
#                 #     lifespan_app=self.app,
#                 # )
#                 # Get all grouped mo_ids
#                 # self.app.store.grouped_elements = (
#                 #     await element_service.get_all_entity_ids()
#                 # )
#                 existed_groups: list[
#                     GroupSchema
#                 ] = await group_service.get_all_groups()
#
#             while True:
#                 if app.store.group_scheme or not existed_groups:
#                     break
#                 elif exist_group_scheme_first_time:
#                     exist_group_scheme_first_time = False
#                     self.logger.error(
#                         "Can't run kafka accessor before dynamic model will be created."
#                     )
#                 await asyncio.sleep(self.start_timeout)
#
#             if self.app.config.kafka.secured:
#                 self.token_provider = AsyncCustomTokenProvider(app=self.app)
#                 await self.token_provider.retrieve_token(first_time=True)
#                 self.task_update_token_provider = asyncio.create_task(
#                     self.token_provider.retrieve_token()
#                 )
#             consumer_config: dict = self.create_config(
#                 kafka_type=KafkaType.AIOKAFKA,
#                 config_type=KafkaConfigType.CONSUMER,
#             )
#             producer_config: dict = self.create_config(
#                 kafka_type=KafkaType.AIOKAFKA,
#                 config_type=KafkaConfigType.PRODUCER,
#             )
#             self.kafka_producer = AIOKafkaProducer(**producer_config)
#             # Check existed topics and create it
#             self.kafka_client = AIOKafkaAdminClient(**producer_config)
#             await self.create_topic()
#             # Add Consumer Groups
#             self.kafka_consumer = AIOKafkaConsumer(**consumer_config)
#
#             self.task_consumer = asyncio.create_task(self.consumer())
#
#             self.buffer_timeout = 5
#
#             auto_group_subscriber = AutoGroupSubscriber(app=app)
#             tmo_subscriber = TMOSubscriber(app=app)
#             tprm_subscriber = TPRMSubscriber(app=app)
#
#             self.buffered_mo_worker = BufferedMoWorker(
#                 timeout_sec=app.config.buffered_mo.KAFKA_BUFFER_TIMEOUT_SEC,
#                 subscribers=auto_group_subscriber,
#             )
#             self.buffered_tmo_worker = BufferedTmoWorker(
#                 timeout_sec=app.config.buffered_mo.KAFKA_BUFFER_TIMEOUT_SEC,
#                 subscribers=tmo_subscriber,
#             )
#             self.buffered_tprm_worker = BufferedTprmWorker(
#                 timeout_sec=app.config.buffered_mo.KAFKA_BUFFER_TIMEOUT_SEC,
#                 subscribers=tprm_subscriber,
#             )
#
#             self.logger.info(msg="started.")
#         except asyncio.CancelledError:
#             self.logger.info(msg="stopped.")
#         except Exception as ex:
#             self.logger.error(msg=f"{type(ex)}: {ex}.")
#
#     async def create_topic(self) -> None:
#         await self.kafka_client.start()
#         set_kafka_topics = set(await self.kafka_client.list_topics())
#         if self.app.config.kafka.topic not in set_kafka_topics:
#             await self.kafka_client.create_topics(
#                 new_topics=[
#                     NewTopic(
#                         name=self.app.config.kafka.topic,
#                         num_partitions=1,
#                         replication_factor=1,
#                     )
#                 ],
#                 timeout_ms=100,
#             )
#         if (
#             self.app.config.kafka.group_data_changes_proto
#             not in set_kafka_topics
#         ):
#             await self.kafka_client.create_topics(
#                 new_topics=[
#                     NewTopic(
#                         name=self.app.config.kafka.group_data_changes_proto,
#                         num_partitions=1,
#                         replication_factor=1,
#                     )
#                 ],
#                 timeout_ms=100,
#             )
#         await self.kafka_client.close()
#
#     async def disconnect(self, app: "Application"):
#         if self.kafka_producer:
#             await self.kafka_producer.stop()
#         if self.task_consumer:
#             self.task_consumer.cancel()
#             await self.task_consumer
#         if self.task_update_buffer:
#             self.task_update_buffer.cancel()
#             await self.task_update_buffer
#         if self.task_update_token_provider:
#             self.task_update_token_provider.cancel()
#         if self.kafka_consumer:
#             await self.kafka_consumer.stop()
#
#     async def consumer(self):
#         try:
#             # Start consumer
#             await self.kafka_consumer.start()
#             if self.app.config.kafka.auto_offset_reset not in [
#                 "latest",
#                 "earliest",
#             ]:
#                 _topic = TopicPartition(
#                     self.app.config.kafka.inventory_topic, 0
#                 )
#                 self.kafka_consumer.assign([_topic])
#                 self.kafka_consumer.seek(
#                     _topic, offset=int(self.app.config.kafka.auto_offset_reset)
#                 )
#                 self.logger.debug(
#                     msg=f"Kafka set offset for {self.app.config.kafka.inventory_topic}."
#                 )
#             else:
#                 self.kafka_consumer.subscribe(
#                     topics=[self.app.config.kafka.inventory_topic]
#                 )
#
#             # Processing new messages
#             async for message in self.kafka_consumer:
#                 if self._exam_new_message(message):
#                     continue
#                 # Created new process
#                 # if self.msg_class_name == "Process":
#                 #     self.buffer.append((self.msg_object.mo_id, self.msg_event))
#                 #     if len(self.buffer) == 1000:
#                 #         await self._clean_buffer()
#                 #     continue
#                 for obj in self.msg_object.objects:
#                     if (
#                         self.msg_class_name == "TMO"
#                         and self.msg_event == "deleted"
#                         and self.buffered_tmo_worker
#                     ):
#                         self.buffered_tmo_worker.notify(
#                             message_type=self.msg_class_name,
#                             action=self.msg_event,
#                             messages=[obj],
#                         )
#                     elif (
#                         self.msg_class_name in {"MO", "PRM"}
#                         and self.buffered_mo_worker
#                     ):
#                         self.buffered_mo_worker.notify(
#                             message_type=self.msg_class_name,
#                             action=self.msg_event,
#                             messages=[obj],
#                         )
#                     elif (
#                         self.msg_class_name == "TPRM"
#                         and self.buffered_tprm_worker
#                     ):
#                         self.buffered_tprm_worker.notify(
#                             message_type=self.msg_class_name,
#                             action=self.msg_event,
#                             messages=[obj],
#                         )
#                 await self.kafka_consumer.commit()
#         except TypeError as ex:
#             self.logger.error(msg=f"Kafka consumer: {type(ex)}: {ex}.")
#         except asyncio.CancelledError:
#             await self.kafka_consumer.stop()
#         except Exception as ex:
#             self.logger.error(msg=f"Kafka consumer error: {type(ex)}: {ex}.")
#
#     async def send_message_about_group_entity(
#         self, data: GroupForKafka, action: str
#     ):
#         self.logger.info(
#             msg=f"Send message to Kafka with {action=} and {data=}"
#         )
#         chunks = [
#             data.entity_ids[i : i + self.msg_size]
#             for i in range(0, len(data.entity_ids), self.msg_size)
#         ]
#         await self.kafka_producer.start()
#         try:
#             # Incorrect work with context manager https://github.com/aio-libs/aiokafka/discussions/925
#             # async with self.kafka_producer as producer:
#             message = group.GROUP()
#             message.group_name = data.group_name
#             message.group_type = data.group_type
#             if chunks:
#                 for chunk in chunks:
#                     message.entity_id.extend(chunk)
#                     await self.kafka_producer.send_and_wait(
#                         topic=self.app.config.kafka.KAFKA_TOPIC,
#                         value=message.SerializeToString(),
#                         key=bytes(action, "utf-8"),
#                     )
#             else:
#                 await self.kafka_producer.send_and_wait(
#                     topic=self.app.config.kafka.KAFKA_TOPIC,
#                     value=message.SerializeToString(),
#                     key=bytes(action, "utf-8"),
#                 )
#             await self.kafka_producer.flush()
#         except aiokafka.errors.ProducerClosed as ex:
#             self.logger.exception(ex)
#         except asyncio.CancelledError:
#             await self.kafka_producer.stop()
#             self.logger.warning(msg="Kafka producer stopped.")
#         except AttributeError as ex:
#             self.logger.warning(
#                 "Error occurred in method 'send_message_about_group_entity'."
#             )
#             self.logger.exception(ex)
#         except Exception as ex:
#             self.logger.exception(ex)
#             raise ex
#
#     async def send_message_about_group_statistic(self, message, action: str):
#         await self.kafka_producer.start()
#         self.logger.info("Send statistic to Kafka:\n%s", message)
#         try:
#             # Incorrect work with context manager https://github.com/aio-libs/aiokafka/discussions/925
#             await self.kafka_producer.send_and_wait(
#                 topic=self.app.config.kafka.KAFKA_GROUP_DATA_CHANGES_PROTO,
#                 value=message.SerializeToString(),
#                 key=bytes(action, "utf-8"),
#             )
#             await self.kafka_producer.flush()
#
#         except aiokafka.errors.ProducerClosed as ex:
#             print(ex.args, ex)
#         except asyncio.CancelledError:
#             await self.kafka_producer.stop()
#             self.logger.warning(msg="producer stopped.")
#         except Exception as ex:
#             self.logger.exception(ex)
#             raise ex
#
#     def create_config(
#         self, kafka_type: KafkaType, config_type: KafkaConfigType
#     ) -> dict:
#         config_dict = dict()
#         match kafka_type:
#             case KafkaType.AIOKAFKA:
#                 config_dict.update(
#                     self.app.config.kafka.model_dump(
#                         include={
#                             "bootstrap_servers": self.app.config.kafka.bootstrap_servers,
#                             "security_protocol": self.app.config.kafka.security_protocol,
#                             "sasl_mechanism": self.app.config.kafka.sasl_mechanism,
#                         }
#                     )
#                 )
#                 if self.app.config.kafka.secured:
#                     config_dict["sasl_oauth_token_provider"] = (
#                         self.token_provider
#                     )
#                 else:
#                     config_dict["security_protocol"] = "PLAINTEXT"
#                     config_dict.pop("sasl_mechanism")
#                 if config_type == config_type.CONSUMER:
#                     config_dict["group_id"] = self.app.config.kafka.group_id
#                     config_dict["auto_offset_reset"] = (
#                         self.app.config.kafka.auto_offset_reset
#                     )
#                     config_dict["enable_auto_commit"] = (
#                         self.app.config.kafka.enable_auto_commit,
#                     )
#             case KafkaType.KAFKA_PYTHON:
#                 config_dict.update(self.app.config.KAFKA_KAFKA_PYTHON_CONFIG)
#                 if not self.app.config.kafka.secured:
#                     config_dict.pop("security.protocol")
#                     config_dict.pop("sasl.mechanism")
#         return config_dict
#
#     def _exam_new_message(self, message: ConsumerRecord) -> bool:
#         result = False
#         # MO:updated
#         message_key = message.key.decode("utf-8")
#         if message_key.find(":") == -1:
#             result = True
#             return result
#         # MO, updated
#         self.msg_class_name, self.msg_event = message_key.split(":")
#         if self.msg_class_name not in kafka_protobuf_message_type.keys():
#             result = True
#             return result
#         if (
#             self.msg_class_name in {"PRM", "TPRM", "Process", "MO", "TMO"}
#             and self.msg_event in kafka_protobuf_message_action.values()
#         ):
#             self.msg_object = kafka_protobuf_message_type[self.msg_class_name]()
#             self.msg_object.ParseFromString(message.value)
#             return result
#         else:
#             result = True
#             return result
#
#     async def _update_dynamic_group(
#         self, current_group: GroupModel, new_data: list[BaseModel]
#     ):
#         # Diff current_group.elements and new_data
#
#         # Remove elements if current_group.elements > new_data
#         set_current_element = {el.entity_id for el in current_group.elements}
#         set_new_element = {el.id for el in new_data}
#         list_ids_for_remove = list(set_current_element - set_new_element)
#         # Add elements if new_data > current_group
#         list_ids_for_create = list(set_new_element - set_current_element)
#
#         # Update DB
#         elements_add_to_db: list[ElementReadyToDB] = []
#         for element in list_ids_for_create:
#             data = {"entity_id": element, "group_id": current_group.id}
#             elements_add_to_db.append(ElementReadyToDB(**data))
#         elements_remove_from_db: list[ElementReadyToDB] = []
#         for element in list_ids_for_remove:
#             data = {"entity_id": element, "group_id": current_group.id}
#             elements_remove_from_db.append(ElementReadyToDB(**data))
#
#         async with self.app.database.session() as session:
#             if list_ids_for_create:
#                 await crud_element.create_element(
#                     session=session, obj_in=elements_add_to_db
#                 )
#             if list_ids_for_remove:
#                 await crud_element.delete(
#                     session=session,
#                     group_id=current_group.id,
#                     obj_in=elements_remove_from_db,
#                 )
#
#         if list_ids_for_create:
#             # Update redis
#             add_to_redis_data = [
#                 el for el in new_data if el.id in list_ids_for_create
#             ]
#             print(f"{add_to_redis_data=}")
#             await self.app.store.redis.set_statistic_by_schema(
#                 current_group=current_group.to_schema(), data=add_to_redis_data
#             )
#             # Update Kafka
#             group_for_kafka = GroupForKafka(
#                 **{
#                     "group_name": current_group.group_name,
#                     "entity_ids": list_ids_for_create,
#                     "group_type": current_group.group_type.name,
#                 }
#             )
#             await self.send_message_about_group_entity(
#                 data=group_for_kafka, action="group:add"
#             )
#         if list_ids_for_remove:
#             # Update redis
#             await self.app.store.redis.delete_values(
#                 group_name=current_group.group_name,
#                 entity_ids=list_ids_for_remove,
#             )
#             # Update Kafka
#             group_for_kafka = GroupForKafka(
#                 **{
#                     "group_name": current_group.group_name,
#                     "entity_ids": list_ids_for_remove,
#                     "group_type": current_group.group_type.name,
#                 }
#             )
#             await self.send_message_about_group_entity(
#                 data=group_for_kafka, action="group:remove"
#             )
#         # statistic: BaseModel = await self.app.store.redis.get_statistic(group_model=current_group)
#         # Update Zeebe
#         # if current_group.group_type_id == 2:
#         #     if current_group.group_process_instance_key:
#         #         # Update statistic
#         #         await self.app.store.grpc.update_process_instance_variables(
#         #             obj_in=statistic, process_instance_key=current_group.group_process_instance_key
#         #         )
#         #     else:
#         #         process_instance_key = await self.app.store.grpc.zeebe_create_process_instance(
#         #             obj_in=statistic, tmo_id=current_group.tmo_id
#         #         )
#         #         update group model - add process instance key for group
#         #         await crud_group.update_group_process_id(
#         #             session=session, obj_in=current_group, process_id=process_instance_key
#         #         )
#
#     async def log_memory_usage_old(self):
#         tracemalloc.start()
#         while True:
#             process = psutil.Process()
#             mem_info = process.memory_info()
#             self.logger.info(
#                 f"Memory usage: {mem_info.rss / 1024 / 1024:.2f} MB"
#             )
#             snapshot = tracemalloc.take_snapshot()
#             top_stats = snapshot.statistics("lineno")
#
#             for stat in top_stats[:10]:
#                 frame = stat.traceback[0]
#                 self.logger.info(
#                     f"{frame.filename}:{frame.lineno} - {stat.size / 1024:.2f} KB"
#                 )
#             self.logger.info("-" * 30)
#             await asyncio.sleep(30)
#
#     def should_include_trace(self, trace):
#         frame = trace[0]
#         filename = frame.filename
#
#         return not any(
#             ignored_path in filename for ignored_path in IGNORE_PATHS
#         )
#
#     async def log_memory_usage(self):
#         tracemalloc.start()
#         previous_snapshot = tracemalloc.take_snapshot()
#         while True:
#             try:
#                 await asyncio.sleep(30)  # Ждем 30 секунд
#                 current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 print(f"\n[Memory Usage Analysis at {current_time}]")
#
#                 current_snapshot = tracemalloc.take_snapshot()
#
#                 stats = current_snapshot.compare_to(previous_snapshot, "lineno")
#                 filtered_stats = [
#                     stat
#                     for stat in stats
#                     if self.should_include_trace(stat.traceback)
#                 ]
#
#                 for stat in filtered_stats[:5]:
#                     frame = stat.traceback[0]
#                     self.logger.info(
#                         f"{frame.filename}:{frame.lineno}: "
#                         f"{stat.size_diff / 1024:,.1f} KB "
#                         f"({stat.count_diff:+d} objects)"
#                     )
#
#                 current, peak = tracemalloc.get_traced_memory()
#                 self.logger.info(
#                     f"Current memory usage: {current / 1024 / 1024:.2f} MB"
#                 )
#                 self.logger.info(
#                     f"Peak memory usage: {peak / 1024 / 1024:.2f} MB"
#                 )
#                 self.logger.info("-" * 30)
#
#                 previous_snapshot = current_snapshot
#
#             except asyncio.CancelledError:
#                 break
#             except Exception as e:
#                 print(f"Error in memory monitoring: {e}")
#                 await asyncio.sleep(30)
