from typing import Sequence

from crud.crud_element import crud_element
from crud.crud_group import crud_group
from crud.element import ElementService
from fastapi import APIRouter, Depends, HTTPException, Request, status
from models.model_element import ElementModel
from models.model_group import GroupModel
from pydantic import BaseModel
from schemas.schema_element import (
    ElementBase,
    ElementReadyToDB,
    ElementResponse,
    ElementSchema,
)
from schemas.schema_group import GroupForKafka, GroupSchema
from sqlalchemy.ext.asyncio import AsyncSession
from store.kafka.protobuf import statistic_pb2

from api import utils
from api.api_v1.endpoints.utils.elements_utils import (
    format_data_from_model_to_kafka_message_for_statistic,
)

router = APIRouter(prefix="/elements", tags=["Elements"])


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=list[ElementResponse],
)
async def add_elements_to_group(
    *,
    session: AsyncSession = Depends(utils.get_session),
    elements: set[ElementBase],
    group_name: str,
    request: Request,
) -> list[ElementResponse]:
    # Get set elements. Check intersection. Validate tmo_id. Save to DB. Recalculate statistic.
    try:
        # Get information about group
        current_group: GroupSchema = await crud_group.get_group_schema(
            session=session, data=group_name
        )
        if not current_group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
            )
        if current_group.column_filters:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You try to add element to dynamic group.",
            )
        element_service = ElementService(
            repo_group=crud_group,
            repo_element=crud_element,
            session=session,
            lifespan_app=request.state.lifespan_app,
        )
        new_elements: list[
            ElementResponse
        ] = await element_service.add_elements(
            group=current_group,
            input_elements=set(el.entity_id for el in elements),
        )
    except (RuntimeError, ValueError) as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error: {type(ex)}: {ex}.",
        )
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Get info: {type(ex)}: {ex}.",
        )
    return new_elements


@router.get(
    "/{entity_id}",
    status_code=status.HTTP_200_OK,
    response_model=list[ElementSchema],
)
async def get_element_by_id(
    *, session: AsyncSession = Depends(utils.get_session), entity_id: int
):
    element: Sequence[ElementModel] = await crud_element.select_elements(
        session=session, elements=[entity_id]
    )
    if not element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Elements not found."
        )
    return element


@router.get(
    "/group/{group_name}",
    status_code=status.HTTP_200_OK,
    response_model=list[ElementSchema],
)
async def get_all_elements_in_group(
    *, session: AsyncSession = Depends(utils.get_session), group_name: str
):
    current_group: GroupModel = await crud_group.get_group(
        session=session, data=group_name
    )
    if current_group:
        elements: Sequence[
            ElementModel
        ] = await crud_element.select_by_group_id(
            session=session, group_id=current_group.id
        )
        return elements
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
        )


@router.delete(
    "/", status_code=status.HTTP_200_OK, response_model=list[ElementSchema]
)
async def remove_element_from_group(
    *,
    session: AsyncSession = Depends(utils.get_session),
    elements: list[ElementBase],
    group_name: str,
    request: Request,
):
    current_group: GroupModel = await crud_group.get_group(
        session=session, data=group_name
    )
    if not current_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
        )
    if current_group.column_filters:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You try to remove element from dynamic group.",
        )
    elements_with_group: list[ElementReadyToDB] = []
    for element in elements:
        data = element.model_dump()
        data |= {"group_id": current_group.id}
        elements_with_group.append(ElementReadyToDB(**data))
    deleted_elements: Sequence[ElementModel] = await crud_element.delete(
        session=session, group_id=current_group.id, obj_in=elements_with_group
    )
    list_deleted_elements = [
        element.to_schema() for element in deleted_elements
    ]
    await session.refresh(current_group)
    # Clear Redis for update statistic
    await request.state.lifespan_app.store.redis.delete_values(
        group_name=group_name,
        entity_ids=[el.entity_id for el in list_deleted_elements],
    )
    # Send message to Kafka about remove element from Group
    group_for_kafka = GroupForKafka(
        **{
            "group_name": current_group.group_name,
            "entity_ids": [
                element.entity_id for element in list_deleted_elements
            ],
            "group_type": current_group.group_type.name,
            "tmo_id": current_group.tmo_id,
        }
    )
    await request.state.lifespan_app.store.kafka_prod.send_message_about_group_entity(
        data=group_for_kafka, action="group:remove"
    )
    statistic: BaseModel = (
        await request.state.lifespan_app.store.redis.get_statistic(
            group_model=current_group
        )
    )
    if current_group.group_type_id == 2:
        # If group without elements therefore need to remove information about process instance key and clean
        # db field
        list_elements: Sequence[
            ElementModel
        ] = await crud_element.select_by_group_id(
            session=session, group_id=current_group.id
        )
        # if not list_elements and current_group.group_process_instance_key:
        #     # Remove from Zeebe
        #     await request.state.lifespan_app.store.grpc.cancel_process_instance(
        #         process_instance_key=[current_group.group_process_instance_key]
        #     )
        #     # Remove from Elastic
        #     await request.state.lifespan_app.store.elastic.remove_data_from_elastic(
        #         process_instance_key=[current_group.group_process_instance_key]
        #     )
        if not list_elements:
            # Clean information in DB
            await crud_group.update_group_process_id(
                session=session, obj_in=current_group, process_id=None
            )
            await crud_group.update_valid(
                session=session, obj_in=current_group, is_valid=True
            )
            await request.state.lifespan_app.store.kafka_prod.send_message_about_group_entity(
                data=group_for_kafka, action="group:delete"
            )

            kafka_statistic_format = (
                format_data_from_model_to_kafka_message_for_statistic(
                    statistic=statistic,
                    group_type=current_group.group_type.name,
                )
            )
            await request.state.lifespan_app.store.kafka_prod.send_message_about_group_statistic(
                message=statistic_pb2.Statistic(**kafka_statistic_format),
                action="group_statistic:delete",
            )

        # Update statistic in Zeebe
        else:
            try:
                # await request.state.lifespan_app.store.grpc.update_process_instance_variables(
                #     obj_in=statistic, process_instance_key=current_group.group_process_instance_key
                # )

                kafka_statistic_format = (
                    format_data_from_model_to_kafka_message_for_statistic(
                        statistic=statistic,
                        group_type=current_group.group_type.name,
                    )
                )
                await request.state.lifespan_app.store.kafka_prod.send_message_about_group_statistic(
                    message=statistic_pb2.Statistic(**kafka_statistic_format),
                    action="group_statistic:update",
                )

            except Exception:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Internal API Error.",
                )
    return list_deleted_elements
