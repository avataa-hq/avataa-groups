from logging import getLogger

from api.api_v1.endpoints.utils.elements_utils import (
    format_data_from_model_to_kafka_message_for_statistic,
)
from schemas.schema_group import (
    GroupBase,
    GroupForKafka,
    GroupSchema,
    InputGroupFromUser,
)
from sqlalchemy.ext.asyncio import AsyncSession
from store.kafka.protobuf import statistic_pb2

from crud.crud_element import CRUDElement
from crud.crud_group import CRUDGroup
from crud.crud_group_type import CRUDGroupType


class GroupService:
    def __init__(
        self,
        repo_group: CRUDGroup,
        repo_group_type: CRUDGroupType,
        repo_element: CRUDElement,
        session: AsyncSession,
        lifespan_app,
    ):
        self.group_repo = repo_group
        self.group_type_repo = repo_group_type
        self.element_repo = repo_element
        self.session = session
        self.app = lifespan_app
        self.logger = getLogger("Group Service")

    async def create_group(self, group_info: InputGroupFromUser) -> GroupSchema:
        current_group: (
            GroupSchema | None
        ) = await self.group_repo.get_group_schema(
            session=self.session, data=group_info.group_name
        )
        if current_group:
            raise ValueError(
                f"Group with name {group_info.group_name} already exist"
            )
        group_type_id: int = await self.group_type_repo.get_group_type_id(
            session=self.session, name=group_info.group_type
        )
        if not group_type_id:
            raise ValueError(
                f"Group Type for group {group_info.group_name} not found"
            )
        try:
            # Create dynamic pydantic statistic model (check tmo_id)
            if not self.app.store.group_scheme.get(
                str(group_info.tmo_id), None
            ):
                await self.app.store.grpc.create_dynamic_statistic_model(
                    tmo_id=group_info.tmo_id
                )
            group_schema: GroupBase = GroupBase(
                group_name=group_info.group_name,
                group_type_id=group_type_id,
                tmo_id=group_info.tmo_id,
                column_filters=group_info.columnFilters,
                is_aggregate=group_info.is_aggregate,
                min_qnt=group_info.min_qnt,
            )
            new_group: GroupSchema = (
                await self.group_repo.create_groups_schema(
                    session=self.session, obj_in=[group_schema]
                )
            )[0]
            return new_group

        except (ValueError, RuntimeError) as ex:
            self.logger.error("Group service error: %s", ex)
            raise ex

    # async def remove_many_by_names(
    #     self, group_names: list[str]
    # ) -> list[GroupModel]:
    #     try:
    #         existed_group: list[GroupModel] = []
    #         for group_name in group_names:  # type: str
    #             current_group = await self.group_repo.get_group_with_elements(
    #                 session=self.session, data=group_name
    #             )
    #             if current_group:
    #                 existed_group.append(current_group)
    #         if not existed_group:
    #             return existed_group
    #         # Remove group from DB
    #         deleted_group: list[
    #             GroupModel
    #         ] = await self.group_repo.remove_groups(
    #             session=self.session, obj_in=existed_group
    #         )
    #         # Send message to kafka group.statistic
    #         for current_group_to_del in deleted_group:
    #             statistic = await self.app.store.redis.get_statistic(
    #                 current_group_to_del
    #             )
    #             kafka_statistic_format = (
    #                 format_data_from_model_to_kafka_message_for_statistic(
    #                     statistic=statistic,
    #                     group_type=current_group_to_del.group_type.name,
    #                 )
    #             )
    #             await self.app.store.kafka_prod.send_message_about_group_statistic(
    #                 message=statistic_pb2.Statistic(**kafka_statistic_format),
    #                 action="group_statistic:delete",
    #             )
    #         # Remove statistics from Redis
    #         await self.app.store.redis.remove_groups(
    #             group_names=[cur.group_name for cur in existed_group]
    #         )
    #         # Send message to kafka group
    #         for current_group_to_del in deleted_group:
    #             # if current_group.elements:
    #             group_for_kafka = GroupForKafka(
    #                 **{
    #                     "group_name": current_group_to_del.group_name,
    #                     "entity_ids": [
    #                         element.entity_id
    #                         for element in current_group_to_del.elements
    #                     ],
    #                     "group_type": current_group_to_del.group_type.name,
    #                     "tmo_id": current_group_to_del.tmo_id,
    #                 }
    #             )
    #             await self.app.store.kafka_prod.send_message_about_group_entity(
    #                 data=group_for_kafka, action="group:delete"
    #             )
    #     except Exception as ex:
    #         self.logger.exception(ex)
    #     # self.app.store.grouped_elements.difference_update(list_entity_to_delete)
    #     return deleted_group

    async def remove_many_by_names_by_schema(
        self, group_names: list[str]
    ) -> list[GroupSchema]:
        try:
            existed_group: list[GroupSchema] = []
            for group_name in group_names:  # type: str
                current_group = await self.group_repo.get_group_with_elements(
                    session=self.session, data=group_name
                )
                if current_group:
                    existed_group.append(current_group.to_schema())
            if not existed_group:
                return existed_group
            # Remove group from DB
            deleted_group: list[
                GroupSchema
            ] = await self.group_repo.remove_groups_by_schema(
                session=self.session, obj_in=existed_group
            )
            # Send message to kafka group.statistic
            for current_group_to_del in deleted_group:
                statistic = await self.app.store.redis.get_statistic_by_schema_for_delete(
                    current_group_to_del
                )
                kafka_statistic_format = (
                    format_data_from_model_to_kafka_message_for_statistic(
                        statistic=statistic,
                        group_type=current_group_to_del.group_type.name,
                    )
                )
                await self.app.store.kafka_prod.send_message_about_group_statistic(
                    message=statistic_pb2.Statistic(**kafka_statistic_format),
                    action="group_statistic:delete",
                )
            # Remove statistics from Redis
            await self.app.store.redis.remove_groups(
                group_names=[cur.group_name for cur in existed_group]
            )
            # Send message to kafka group
            for current_group_to_del in deleted_group:
                # if current_group.elements:
                group_for_kafka = GroupForKafka(
                    **{
                        "group_name": current_group_to_del.group_name,
                        "entity_ids": [
                            element.entity_id
                            for element in current_group_to_del.elements
                        ],
                        "group_type": current_group_to_del.group_type.name,
                        "tmo_id": current_group_to_del.tmo_id,
                    }
                )
                await self.app.store.kafka_prod.send_message_about_group_entity(
                    data=group_for_kafka, action="group:delete"
                )
        except Exception as ex:
            self.logger.exception(ex)
        # self.app.store.grouped_elements.difference_update(list_entity_to_delete)
        return deleted_group

    async def get_all_groups(self) -> list[GroupSchema]:
        try:
            result: list[
                GroupSchema
            ] = await self.group_repo.get_all_group_schema(
                session=self.session, limit=10_000
            )
            return result
        except Exception as ex:
            self.logger.exception(ex)

    async def remove_many_by_tmo(self, tmo_ids: list[int]) -> list[GroupSchema]:
        try:
            # Get all elements in removed groups
            list_group_to_delete = (
                await self.group_repo.get_group_with_elements_by_tmo_id(
                    session=self.session, tmo_ids=tmo_ids
                )
            )
            if not list_group_to_delete:
                return []
            result: list[GroupSchema] = await self.group_repo.remove_groups(
                session=self.session, obj_in=list_group_to_delete
            )
        except Exception as ex:
            self.logger.exception(ex)
        # Remove elements from list existed mo
        # self.app.store.grouped_elements.difference_update(list_entity_to_delete)
        return result

    async def get_group_names_by_tmo_id(self, tmo_ids: list[int]) -> list[str]:
        try:
            list_group_names = await self.group_repo.get_group_names_by_tmo(
                session=self.session, tmo_ids=tmo_ids
            )
        except Exception as ex:
            self.logger.exception(ex)
        return list_group_names
