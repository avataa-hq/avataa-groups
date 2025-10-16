import pickle
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import TYPE_CHECKING, AsyncGenerator, Sequence

import grpc
from base.base_accessor import BaseAccessor
from crud.crud_element import crud_element
from crud.crud_group import crud_group
from crud.crud_group_type import crud_group_type
from models.model_element import ElementModel
from models.model_group import GroupModel
from pydantic import BaseModel
from schemas.schema_element import ElementReadyToDB
from schemas.schema_group import GroupBase, GroupForKafka
from sqlalchemy import select

from .protobuf import grpc_group_pb2_grpc
from .protobuf.grpc_group_pb2 import (
    Elements,
    RequestCreateGroup,
    RequestElements,
    RequestGetGroupStatistic,
    RequestGroupByType,
    RequestListGroupByTMOID,
    RequestListGroupName,
    RequestListMOIdsInSpecialGroup,
    ResponseElements,
    ResponseGetGroupStatistic,
    ResponseGroupStatus,
    ResponseListGroupByTMOID,
    ResponseListGroupName,
    ResponseListMOIdsInSpecialGroup,
)

if TYPE_CHECKING:
    from core.app import Application


class GroupGRPC(grpc_group_pb2_grpc.GroupServicer):
    def __init__(self, app: "Application", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = app
        self.logger = getLogger("Group GRPC")

    async def CreateGroups(
        self,
        request: RequestCreateGroup,
        context: grpc.aio.ServicerContext,
    ) -> ResponseGroupStatus | grpc.aio.ServicerContext:
        try:
            status: ResponseGroupStatus = ResponseGroupStatus()
            list_group_schemas: list[GroupBase] = []
            async with self.app.database.session() as session:
                for el in request.group_info:
                    current_group: (
                        GroupModel | None
                    ) = await crud_group.get_group(
                        session=session, data=el.group_name
                    )
                    if current_group:
                        context.set_details("Duplicate group.")
                        context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                        return context
                    group_type_id: (
                        int | None
                    ) = await crud_group_type.get_group_type_id(
                        session=session, name=el.group_type
                    )
                    if not group_type_id:
                        context.set_details("Group Type not found.")
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        return context
                    list_group_schemas.append(
                        GroupBase(
                            group_name=el.group_name,
                            group_type_id=group_type_id,
                            tmo_id=el.tmo_id,
                        )
                    )
                new_group: Sequence[
                    GroupModel
                ] = await crud_group.create_groups(
                    session=session, obj_in=list_group_schemas, app=self.app
                )
            if new_group:
                status.response = True
                return status
            else:
                context.set_details("Groups not found.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return context
        except ValueError as ex:
            context.set_details(f"DB error: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context
        except Exception as ex:
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context

    async def DeleteGroups(
        self,
        request: RequestListGroupName,
        context: grpc.aio.ServicerContext,
    ) -> ResponseGroupStatus | grpc.aio.ServicerContext:
        list_groups: list[GroupModel] = []
        status: ResponseGroupStatus = ResponseGroupStatus()
        try:
            async with self.app.database.session() as session:
                for group_name in request.group_name:
                    list_groups.append(
                        await crud_group.get_group(
                            session=session, data=group_name
                        )
                    )
                if list_groups:
                    # Remove groups from DB
                    deleted_group: Sequence[
                        GroupModel
                    ] = await crud_group.remove_groups(
                        session=session, obj_in=list_groups
                    )
                else:
                    context.set_details("Groups not found.")
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return context
            # Remove statistics from Redis
            await self.app.store.redis.remove_groups(
                group_names=[cur.group_name for cur in list_groups]
            )
            for current_group in list_groups:
                # Send message to kafka
                if current_group.elements:
                    group_for_kafka = GroupForKafka(
                        **{
                            "group_name": current_group.group_name,
                            "entity_ids": [
                                element.entity_id
                                for element in current_group.elements
                            ],
                            "group_type": current_group.group_type.name,
                            "tmo_id": current_group.tmo_id,
                        }
                    )
                    await self.app.store.kafka_prod.send_message_about_group_entity(
                        data=group_for_kafka, action="group:delete"
                    )
                # Remove group from Zeebe
                # if current_group.group_process_instance_key:
                # await self.app.store.grpc.cancel_process_instance(
                #     process_instance_key=[
                #         cur.group_process_instance_key
                #         for cur in list_groups
                #         if cur.group_process_instance_key and cur.group_type_id == 2
                #     ]
                # )
                # Remove process from Elastic
                # await self.app.store.elastic.remove_data_from_elastic(
                #     process_instance_key=[
                #         cur.group_process_instance_key
                #         for cur in list_groups
                #         if cur.group_process_instance_key and cur.group_type_id == 2
                #     ]
                # )
            if deleted_group:
                status.response = True
                return status
        except Exception as ex:
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context

    async def AddElementsToGroup(
        self,
        request: RequestElements,
        context: grpc.aio.ServicerContext,
    ) -> ResponseGroupStatus | grpc.aio.ServicerContext:
        status: ResponseGroupStatus = ResponseGroupStatus()
        input_elements_ready_to_db: list[ElementReadyToDB] = []
        try:
            async with self.app.database.session() as session:
                for element in request.elements:
                    # Get information about group
                    current_group: GroupModel = await crud_group.get_group(
                        session=session, data=element.group_name
                    )
                    if not current_group:
                        context.set_details("Groups not found.")
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        return context
                    list_input_entity_id: list[int] = list(element.entity_id)
                    # Get information about process - get serialized pydantic object and valid
                    if current_group.group_type_id == 1:
                        data_for_statistic: tuple[
                            list[BaseModel], list, list[BaseModel]
                        ] = await self.app.store.grpc.inventory_get_info(
                            current_group=current_group.to_schema(),
                            mo_ids=list_input_entity_id,
                        )
                    # Get information about process from Zeebe for input elements -> move to Search!
                    # else:
                    #     data_for_statistic: tuple[
                    #         list[BaseModel], list, list[BaseModel]
                    #     ] = await self.app.store.grpc.zeebe_get_existed_entities(
                    #         group=current_group, mo_ids=list_input_entity_id
                    #     )
                    data_input_elements, is_valid = data_for_statistic
                    # Update is_valid field in group table
                    if current_group.is_valid != is_valid:
                        await crud_group.update_valid(
                            session=session,
                            obj_in=current_group,
                            is_valid=False,
                        )
                    # Check correct tmo_id in group and input entity
                    for cur_el in data_input_elements[1:]:  # type: BaseModel
                        if cur_el.TMO.tmo_id != current_group.tmo_id:
                            context.set_details(
                                "Different tmo_id for input elements."
                            )
                            context.set_code(
                                grpc.StatusCode.FAILED_PRECONDITION
                            )
                            return context
                    # Get elements from input group
                    existed_elements_in_group: Sequence[
                        ElementModel
                    ] = await crud_element.select_by_group_id(
                        session=session, group_id=current_group.id
                    )
                    # Remove input elements if they existed
                    if existed_elements_in_group:
                        filtered_elements: set[int] = set(
                            list_input_entity_id
                        ) - set(
                            el.entity_id for el in existed_elements_in_group
                        )
                        if not filtered_elements:
                            continue
                        if (
                            current_group.tmo_id
                            != data_input_elements[0].TMO.tmo_id
                        ):
                            continue
                        list_input_entity_id = list(filtered_elements)
                    # Update/create redis key
                    await self.app.store.redis.set_statistic_by_schema(
                        current_group=current_group.to_schema(),
                        data=data_input_elements,
                    )
                    # Send message to Kafka about add element
                    group_for_kafka = GroupForKafka(
                        **{
                            "group_name": current_group.group_name,
                            "entity_ids": list_input_entity_id,
                            "group_type": current_group.group_type.name,
                            "tmo_id": current_group.tmo_id,
                        }
                    )
                    await self.app.store.kafka_prod.send_message_about_group_entity(
                        data=group_for_kafka, action="group:add"
                    )
                    # Send data to Zeebe
                    # if current_group.group_type_id == 2:
                    #     if current_group.group_process_instance_key:
                    #         # Update statistic
                    #         await self.app.store.grpc.update_process_instance_variables(
                    #             obj_in=statistic, process_instance_key=current_group.group_process_instance_key
                    #         )
                    #     else:
                    #         process_instance_key = await self.app.store.grpc.zeebe_create_process_instance(
                    #             obj_in=statistic, tmo_id=current_group.tmo_id
                    #         )
                    #         # update group model - add process instance key for group
                    #         await crud_group.update_group_process_id(
                    #             session=session, obj_in=current_group, process_id=process_instance_key
                    #         )

                    for cur_el in list_input_entity_id:
                        input_elements_ready_to_db.append(
                            ElementReadyToDB(
                                group_id=current_group.id, entity_id=cur_el
                            )
                        )
                # Save info to DB
                new_element = []
                if input_elements_ready_to_db:
                    new_element: Sequence[
                        ElementModel
                    ] = await crud_element.create_element(
                        session=session, obj_in=input_elements_ready_to_db
                    )

            if new_element:
                status.response = True
                return status
            else:
                context.set_details("Entities not grouped.")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return context
        except Exception as ex:
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context

    async def RemoveElementsFromGroup(
        self,
        request: RequestElements,
        context: grpc.aio.ServicerContext,
    ) -> ResponseGroupStatus | grpc.aio.ServicerContext:
        status: ResponseGroupStatus = ResponseGroupStatus()
        try:
            async with self.app.database.session() as session:
                for element in request.elements:
                    if not element.entity_id:
                        context.set_details("Elements not found.")
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        return context
                    # Get information about group
                    current_group: GroupModel = await crud_group.get_group(
                        session=session, data=element.group_name
                    )
                    if not current_group:
                        context.set_details("Groups not found.")
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        return context
                    list_input_entity_id: list[int] = list(element.entity_id)
                    # Serialize elements for delete from DB
                    elements_for_delete: list[ElementReadyToDB] = []
                    for cur_el in list_input_entity_id:
                        elements_for_delete.append(
                            ElementReadyToDB(
                                group_id=current_group.id, entity_id=cur_el
                            )
                        )
                    # Clear Redis for update statistic
                    await self.app.store.redis.delete_values(
                        group_name=current_group.group_name,
                        entity_ids=list_input_entity_id,
                    )
                    # Send message to Kafka about remove element from Group
                    group_for_kafka = GroupForKafka(
                        **{
                            "group_name": current_group.group_name,
                            "entity_ids": list_input_entity_id,
                            "group_type": current_group.group_type.name,
                            "tmo_id": current_group.tmo_id,
                        }
                    )
                    await self.app.store.kafka_prod.send_message_about_group_entity(
                        data=group_for_kafka, action="group:remove"
                    )
                    # Check if Process group will be empty after delete elements
                    # if current_group.group_type_id == 2:
                    #     if (
                    #             not ({el.entity_id for el in current_group.elements} - set(list_input_entity_id))
                    #             and current_group.group_process_instance_key
                    #     ):
                    #         Remove from Zeebe
                    #         await self.app.store.grpc.cancel_process_instance(
                    #             process_instance_key=[current_group.group_process_instance_key]
                    #         )
                    #         # Remove from Elastic
                    #         await self.app.store.elastic.remove_data_from_elastic(
                    #             process_instance_key=[current_group.group_process_instance_key]
                    #         )
                    #         # Clean information in DB
                    #         await crud_group.update_group_process_id(
                    #             session=session, obj_in=current_group, process_id=None
                    #         )
                    #         await crud_group.update_valid(session=session, obj_in=current_group, is_valid=True)
                    #     else:
                    #         statistic: BaseModel = await self.app.store.redis.get_statistic(group_model=current_group)
                    #         await self.app.store.grpc.update_process_instance_variables(
                    #             obj_in=statistic, process_instance_key=current_group.group_process_instance_key
                    #         )
                # Delete elements from DB
                deleted_elements: list[
                    ElementModel
                ] = await crud_element.delete(
                    session=session,
                    group_id=current_group.id,
                    obj_in=elements_for_delete,
                )
            if deleted_elements:
                status.response = True
                return status
            else:
                context.set_details("Groups not deleted.")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return context
        except Exception as ex:
            print(f"{type(ex)}: {ex}")
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context

    async def ExistedGroup(
        self,
        request: RequestListGroupName,
        context: grpc.aio.ServicerContext,
    ) -> ResponseListGroupName | grpc.aio.ServicerContext:
        existed_group: ResponseListGroupName = ResponseListGroupName()
        try:
            async with self.app.database.session() as session:
                for group_name in request.group_name:
                    current_group: GroupModel = await crud_group.get_group(
                        session=session, data=group_name
                    )
                    if current_group:
                        existed_group.group_name.append(
                            current_group.group_name
                        )
        except Exception as ex:
            print(f"{type(ex)}: {ex}")
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context

        return existed_group

    async def ListGroupWithElements(
        self,
        request: RequestGroupByType,
        context: grpc.aio.ServicerContext,
    ) -> ResponseListGroupName | grpc.aio.ServicerContext:
        existed_group: ResponseListGroupName = ResponseListGroupName()
        all_group: list[GroupModel] = []
        limit = 15
        offset = 0
        try:
            async with self.app.database.session() as session:
                while True:
                    result: Sequence[
                        GroupModel
                    ] = await crud_group.get_all_group_by_type(
                        session=session,
                        group_type_id=request.group_type + 1,
                        limit=limit,
                        offset=offset,
                    )
                    all_group.extend(result)
                    if len(result) < offset + limit:
                        break
                    offset += limit
                for group in all_group:
                    current_group: GroupModel = await crud_group.get_group(
                        session=session, data=group.group_name
                    )
                    if current_group.elements:
                        existed_group.group_name.append(group.group_name)
        except Exception as ex:
            print(f"{type(ex)}: {ex}")
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context
        return existed_group

    async def ListElementsInGroups(
        self,
        request: RequestListGroupName,
        context: grpc.aio.ServicerContext,
    ) -> ResponseElements | grpc.aio.ServicerContext:
        groups_with_element: ResponseElements = ResponseElements()
        try:
            async with self.app.database.session() as session:
                for group_name in request.group_name:
                    current_group: GroupModel = await crud_group.get_group(
                        session=session, data=group_name
                    )
                    if current_group:
                        current_elements = Elements(
                            group_name=current_group.group_name,
                            entity_id=[
                                el.entity_id for el in current_group.elements
                            ],
                        )
                        groups_with_element.elements.append(current_elements)
        except Exception as ex:
            print(f"{type(ex)}: {ex}")
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context
        return groups_with_element

    async def ListGroupByTMOID(
        self,
        request: RequestListGroupByTMOID,
        context: grpc.aio.ServicerContext,
    ) -> AsyncGenerator:
        """Returns stream of names of groups for special TMO"""
        group_names_per_step = 10000

        async with self.app.database.session() as session:
            stmt = (
                select(GroupModel.group_name)
                .where(GroupModel.tmo_id == request.tmo_id)
                .execution_options(yield_per=group_names_per_step)
            )
            search_res = await session.stream(stmt)

            async for part_data in search_res.scalars().partitions(
                group_names_per_step
            ):
                yield ResponseListGroupByTMOID(group_names=part_data)

    async def ListMOIdsInSpecialGroup(
        self,
        request: RequestListMOIdsInSpecialGroup,
        context: grpc.aio.ServicerContext,
    ) -> AsyncGenerator:
        entity_ids_per_step = 10000

        async with self.app.database.session() as session:
            stmt = select(GroupModel).where(
                GroupModel.group_name == request.group_name
            )
            group = await session.execute(stmt)
            group = group.scalars().first()

            if not group:
                yield ResponseListMOIdsInSpecialGroup(entity_ids=list())
                return

            stmt = (
                select(ElementModel.entity_id)
                .where(ElementModel.group_id == group.id)
                .execution_options(yield_per=entity_ids_per_step)
            )
            search_res = await session.stream(stmt)

            async for part_data in search_res.scalars().partitions(
                entity_ids_per_step
            ):
                yield ResponseListMOIdsInSpecialGroup(entity_ids=part_data)

    async def GetGroupStatistic(
        self,
        request: RequestGetGroupStatistic,
        context: grpc.aio.ServicerContext,
    ) -> ResponseGetGroupStatistic | grpc.aio.ServicerContext:
        async with self.app.database.session() as session:
            group_model = await crud_group.get_group(
                session=session, data=request.group_name
            )

        if not group_model:
            return ResponseGetGroupStatistic(group_statistic="")
        try:
            statistic = await self.app.store.redis.get_statistic(
                group_model=group_model
            )
        except Exception as ex:
            print(f"GetGroupStatistic Error{type(ex)}: {ex}")
            context.set_details(f"{type(ex)}: {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return context
        temp_statistic = statistic.model_dump()
        temp_statistic["group_type"] = group_model.group_type.name
        pickled_data = pickle.dumps(temp_statistic).hex()
        return ResponseGetGroupStatistic(group_statistic=pickled_data)


class GRPCServer(BaseAccessor):
    def __init__(self, app: "Application", *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        self.logger = getLogger("gRPC_Server")
        self.server: grpc.aio.Server | None = None

    async def connect(self, app: "Application"):
        self.server = grpc.aio.server(ThreadPoolExecutor(max_workers=10))
        self.server.add_insecure_port(
            f"[::]:{app.config.grpc.SERVER_GRPC_PORT}"
        )
        grpc_group_pb2_grpc.add_GroupServicer_to_server(
            GroupGRPC(app=app), self.server
        )
        await self.server.start()
        self.logger.info(
            "started at port %d.", app.config.grpc.SERVER_GRPC_PORT
        )

    async def disconnect(self, app: "Application"):
        # grpc 1.64 is experimental API and not correct stop grpc server
        await self.server.wait_for_termination(1)
        await self.server.stop(grace=1.0)
        self.logger.info(msg="stopped.")
