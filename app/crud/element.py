from logging import getLogger
from typing import Sequence

from api.api_v1.endpoints.utils.elements_utils import (
    format_data_from_model_to_kafka_message_for_statistic,
)
from pydantic import BaseModel
from schemas.schema_element import (
    ElementReadyToDB,
    ElementResponse,
    ElementSchema,
)
from schemas.schema_group import GroupForKafka, GroupSchema
from sqlalchemy.ext.asyncio import AsyncSession
from store.kafka.protobuf import statistic_pb2

from crud.crud_element import CRUDElement
from crud.crud_group import CRUDGroup


class ElementService:
    def __init__(
        self,
        repo_group: CRUDGroup,
        repo_element: CRUDElement,
        session: AsyncSession,
        lifespan_app,
    ):
        self.group_repo = repo_group
        self.element_repo = repo_element
        self.session = session
        self.app = lifespan_app
        self.logger = getLogger("Element Service")

    async def add_elements(
        self, group: GroupSchema, input_elements: set[int] | None = None
    ) -> list[ElementResponse]:
        try:
            if group.group_type_id == 1:
                statistic_data: tuple[
                    list[BaseModel], bool
                ] = await self.app.store.grpc.inventory_get_info(
                    current_group=group, mo_ids=list(input_elements)
                )
            # Get information about process from Search MS for group
            elif group.group_type_id == 2:
                all_elements = []
                if group.elements:
                    all_elements += [el.entity_id for el in group.elements]
                if input_elements:
                    all_elements += list(input_elements)
                statistic_data: tuple[
                    list[BaseModel], list, list[BaseModel]
                ] = await self.app.store.grpc.get_severity_processes(
                    group_schema=group, mo_ids=all_elements
                )
            else:
                raise ValueError("Incorrect group type.")
        except RuntimeError as ex:
            self.logger.error("Cannot get data from external service.")
            raise ex
        except ValueError as ex:
            self.logger.exception(ex)
            raise ex
        statistic_model, is_valid = statistic_data

        if not input_elements:
            input_elements: set[int] = set(el.MO.id for el in statistic_model)
        # Get existed elements from input group
        # existed_elements_in_group: list[ElementSchema] = group.elements
        # existed_elements_in_group: list[ElementSchema] = await self.element_repo.select_by_group_id_schema(
        #     session=self.session, group_id=group.id
        # )
        # Remove elements from input if they existed in current group
        if group.elements:
            ids_to_add: set[int] = input_elements - set(
                el.entity_id for el in group.elements
            )
        else:
            ids_to_add = input_elements
        if ids_to_add and (
            len(group.elements) + len(ids_to_add) > (group.min_qnt or 0)
        ):
            statistic = await self.app.store.redis.set_statistic_by_schema(
                current_group=group, data=statistic_model
            )
            element_response: list[
                ElementResponse
            ] = await self._update_info_about_group(
                new_ids=ids_to_add,
                is_valid=is_valid,
                group=group,
                statistic=statistic,
            )
            # Add element to grouped elements
            # self.app.store.grouped_elements.update(input_elements)
            return element_response
        return []

    async def _update_info_about_group(
        self,
        new_ids: set[int],
        is_valid: bool,
        group: GroupSchema,
        statistic: BaseModel,
    ):
        try:
            input_elements_ready_to_db: list[ElementReadyToDB] = []
            for element_id in new_ids:
                data = {"entity_id": element_id, "group_id": group.id}
                input_elements_ready_to_db.append(ElementReadyToDB(**data))

            new_element: list[
                ElementSchema
            ] = await self.element_repo.create_element_schema(
                session=self.session, obj_in=input_elements_ready_to_db
            )
            # Update is_valid
            if group.is_valid != is_valid:
                await self.group_repo.update_valid_schema(
                    session=self.session, obj_in=group, is_valid=bool(is_valid)
                )
            # Send message to Kafka group about add element
            group_for_kafka = GroupForKafka(
                **{
                    "group_name": group.group_name,
                    "entity_ids": list(new_ids),
                    "group_type": group.group_type.name,
                    "tmo_id": group.tmo_id,
                }
            )
            await self.app.store.kafka_prod.send_message_about_group_entity(
                data=group_for_kafka, action="group:add"
            )

            # Send message to Kafka group statistic
            kafka_statistic_format = (
                format_data_from_model_to_kafka_message_for_statistic(
                    statistic=statistic, group_type=group.group_type.name
                )
            )
            await self.app.store.kafka_prod.send_message_about_group_statistic(
                message=statistic_pb2.Statistic(**kafka_statistic_format),
                action="group_statistic:update",
            )
        except AttributeError as ex:
            self.logger.error("Element Service Error: %s", ex)
            self.logger.exception(ex)
            raise ValueError(ex)
        except Exception as ex:
            self.logger.error("Element Service Error: %s", ex)
            self.logger.exception(ex)
            raise ValueError(f"Update group info: {ex}")

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
        return response_new_element

    async def get_all_entity_ids(self) -> set[int]:
        try:
            result: Sequence[
                int
            ] = await self.element_repo.select_all_entity_id(
                session=self.session
            )
        except Exception as ex:
            self.logger.exception(ex)
        return set(result)
