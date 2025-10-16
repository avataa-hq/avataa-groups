from typing import Annotated, Sequence

from crud.crud_element import crud_element
from crud.crud_group import crud_group
from crud.crud_group_type import crud_group_type
from crud.element import ElementService
from crud.group import GroupService
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from models.model_group import GroupModel
from pydantic import BaseModel
from schemas.schema_group import (
    GroupResponse,
    GroupSchema,
    InputGroupFromUser,
)
from sqlalchemy.ext.asyncio import AsyncSession
from store.kafka.protobuf import statistic_pb2

from api import utils

from .utils.elements_utils import (
    format_data_from_model_to_kafka_message_for_statistic,
)

router = APIRouter(prefix="/groups", tags=["Groups"])


class GetQueryParams:
    def __init__(
        self, group_id: int | None = None, group_name: str | None = None
    ):
        self.data = None
        if group_id:
            self.data = group_id
        elif group_name:
            self.data = group_name


class GetPagingParams:
    def __init__(self, offset: int = 0, limit: int = 15):
        self.offset = offset
        self.limit = limit


@router.get(
    "/all", status_code=status.HTTP_200_OK, response_model=list[GroupSchema]
)
async def get_all_groups(
    *,
    limit: int = 300,
    offset: int = 0,
    session: AsyncSession = Depends(utils.get_session),
):
    try:
        groups: Sequence[GroupModel] = await crud_group.get_all_group(
            session=session, limit=limit, offset=offset
        )
        return groups
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"{type(ex)}: {ex}."
        )


@router.get(
    "/by_type/{group_type}",
    status_code=status.HTTP_200_OK,
    response_model=GroupResponse,
)
async def get_all_groups_by_type(
    *,
    group_type: str,
    limit: int = 50,
    offset: int = 0,
    session: AsyncSession = Depends(utils.get_session),
):
    try:
        group_type_id = await crud_group_type.get_group_type_id(
            session=session, name=group_type
        )
        groups: Sequence[GroupModel] = await crud_group.get_all_group_by_type(
            session=session,
            group_type_id=group_type_id,
            limit=limit,
            offset=offset,
        )
        total = await crud_group.get_count_group(
            session=session, group_type_id=group_type_id
        )
        result = GroupResponse(groups=groups, total=total)
        return result
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"{type(ex)}: {ex}."
        )


@router.post(
    "/", status_code=status.HTTP_201_CREATED, response_model=GroupSchema
)
async def create_group(
    *,
    session: AsyncSession = Depends(utils.get_session),
    group_info: Annotated[InputGroupFromUser, Body(embed=True)],
    request: Request,
):
    try:
        group_service = GroupService(
            repo_group=crud_group,
            repo_group_type=crud_group_type,
            repo_element=crud_element,
            session=session,
            lifespan_app=request.state.lifespan_app,
        )
        new_group: GroupSchema = await group_service.create_group(
            group_info=group_info
        )
        # Add dynamic elements to group
        if new_group.column_filters:
            element_service = ElementService(
                repo_group=crud_group,
                repo_element=crud_element,
                session=session,
                lifespan_app=request.state.lifespan_app,
            )
            await element_service.add_elements(group=new_group)
        return new_group
    except (ValueError, RuntimeError) as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Group created with error: {ex}.",
        )
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Internal error during group creation: {ex}.",
        )


@router.post("/statistic/", status_code=status.HTTP_200_OK)
async def get_group_statistic(
    *,
    session: AsyncSession = Depends(utils.get_session),
    commons: Annotated[GetQueryParams, Depends()],
    request: Request,
):
    current_group: GroupModel = await crud_group.get_group_with_elements(
        session=session, data=commons.data
    )
    if current_group:
        try:
            group_statistic: BaseModel = (
                await request.state.lifespan_app.store.redis.get_statistic(
                    group_model=current_group
                )
            )
            return group_statistic
        except Exception as ex:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Group statistic doesn't exist. {ex}",
            )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
    )


@router.post("/statistic/reset", status_code=status.HTTP_200_OK)
async def reset_group_statistic(
    *,
    session: AsyncSession = Depends(utils.get_session),
    commons: Annotated[GetQueryParams, Depends()],
    request: Request,
):
    # Remove current statistic for group. Create new statistic founded on current group elements.
    # Update information in Camunda.
    current_group: GroupModel = await crud_group.get_group_with_elements(
        session=session, data=commons.data
    )
    if not current_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
        )
    if not current_group.elements:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Group without elements.",
        )
    await request.state.lifespan_app.store.redis.remove_groups(
        group_names=[current_group.group_name]
    )
    try:
        statistic = await request.state.lifespan_app.store.redis.get_statistic(
            group_model=current_group
        )
        kafka_statistic_format = (
            format_data_from_model_to_kafka_message_for_statistic(
                statistic=statistic, group_type=current_group.group_type.name
            )
        )
        await request.state.lifespan_app.store.kafka_prod.send_message_about_group_statistic(
            message=statistic_pb2.Statistic(**kafka_statistic_format),
            action="group_statistic:update",
        )
        return statistic
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"{ex}."
        )


@router.delete(
    "/", status_code=status.HTTP_200_OK, response_model=list[GroupSchema]
)
async def delete_groups(
    *,
    session: AsyncSession = Depends(utils.get_session),
    group_names: list[str],
    request: Request,
):
    try:
        group_service = GroupService(
            repo_group=crud_group,
            repo_group_type=crud_group_type,
            repo_element=crud_element,
            session=session,
            lifespan_app=request.state.lifespan_app,
        )
        result: list[
            GroupSchema
        ] = await group_service.remove_many_by_names_by_schema(
            group_names=group_names
        )
        return result
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"{ex}"
        )
