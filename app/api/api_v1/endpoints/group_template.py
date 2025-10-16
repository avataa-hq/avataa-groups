from typing import Annotated

from crud.crud_element import crud_element
from crud.crud_group import crud_group
from crud.crud_group_template import crud_group_template
from crud.crud_group_type import crud_group_type
from crud.group import GroupService
from crud.group_template import GroupTemplateService
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from models.model_group_template import GroupTemplateModel
from schemas.schema_group import GroupBase
from schemas.schema_group_template import (
    GroupTemplateCreate,
    GroupTemplateMain,
    GroupTemplateSchema,
)
from sqlalchemy.ext.asyncio import AsyncSession

from api import utils

router = APIRouter(prefix="/templates", tags=["GroupTemplate"])


class GetPagingParams:
    def __init__(self, offset: int = 0, limit: int = 15) -> None:
        self.limit = limit
        self.offset = offset


@router.post(
    "/", status_code=status.HTTP_201_CREATED, response_model=GroupTemplateSchema
)
async def create_group_template(
    *,
    session: AsyncSession = Depends(utils.get_session),
    group_template_info: Annotated[GroupTemplateCreate, Body(embed=True)],
    request: Request,
):
    if (
        group_template_info.column_filters == []
        and group_template_info.ranges_object == {}
        and group_template_info.identical == []
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Group templates creation error. Too broad case",
        )
    current_group_template: (
        GroupTemplateModel | None
    ) = await crud_group_template.get_group_template(
        session=session, data=group_template_info.name
    )
    if current_group_template:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Duplicate group template name.",
        )
    group_type_id: int | None = await crud_group_type.get_group_type_id(
        session=session, name=group_template_info.group_type_name
    )
    if not group_type_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group Type for template not found.",
        )
    # fix correct ranges
    ranges = {"ranges": {}}
    if group_template_info.ranges_object:
        ranges["ranges"] = group_template_info.ranges_object
    group_template_main = GroupTemplateMain(
        name=group_template_info.name,
        column_filters=group_template_info.column_filters,
        ranges_object=ranges,
        identical=group_template_info.identical,
        min_qnt=group_template_info.min_qnt,
        tmo_id=group_template_info.tmo_id,
        group_type_id_for_template=group_type_id,
    )
    try:
        # check filters/ranges/identical before creation
        if group_template_info.identical:
            await request.state.lifespan_app.store.grpc.get_processes_group_from_search(
                group_template=group_template_main
            )
        else:
            group_schema: GroupBase = GroupBase(
                group_name="_temp",
                group_type_id=group_type_id,
                tmo_id=group_template_info.tmo_id,
                column_filters=group_template_info.column_filters,
                ranges_object=ranges,
                min_qnt=group_template_info.min_qnt,
            )
            # Check if group tmo id scheme exist
            if not request.state.lifespan_app.store.group_scheme.get(
                str(group_schema.tmo_id), None
            ):
                await request.state.lifespan_app.store.grpc.create_dynamic_statistic_model(
                    tmo_id=group_schema.tmo_id
                )
            await request.state.lifespan_app.store.grpc.get_severity_processes(
                group_schema=group_schema
            )
        new_group_template: list[
            GroupTemplateModel
        ] = await crud_group_template.create_group_templates(
            session=session, obj_in=[group_template_main]
        )
        return new_group_template[0]
    except (ValueError, RuntimeError) as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Group templates creation error: {ex}.",
        )
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Internal error: {ex}.",
        )


@router.get(
    "/all",
    status_code=status.HTTP_200_OK,
    response_model=list[GroupTemplateSchema],
)
async def get_all_group_template(
    *,
    session: AsyncSession = Depends(utils.get_session),
    paging: Annotated[GetPagingParams, Depends()],
):
    list_group_templates: list[
        GroupTemplateModel
    ] = await crud_group_template.get_all_group_template(
        session=session, limit=paging.limit, offset=paging.offset
    )
    return list_group_templates


@router.delete(
    "/",
    status_code=status.HTTP_200_OK,
    response_model=list[GroupTemplateSchema],
)
async def delete_group_template(
    *,
    session: AsyncSession = Depends(utils.get_session),
    group_for_delete: list[int],
    request: Request,
):
    if not group_for_delete:
        return []
    group_template_service = GroupTemplateService(
        repo_group_template=crud_group_template,
        session=session,
        lifespan_app=request.state.lifespan_app,
    )
    group_service = GroupService(
        repo_group=crud_group,
        repo_group_type=crud_group_type,
        repo_element=crud_element,
        session=session,
        lifespan_app=request.state.lifespan_app,
    )

    list_group_templates: list[
        GroupTemplateModel
    ] = await group_template_service.get_group_template_by_ids(
        template_ids=group_for_delete
    )

    for group_template in list_group_templates:
        await group_service.remove_many_by_names_by_schema(
            group_names=[
                group.group_name for group in group_template.groups_for_template
            ]
        )
    deleted_groups_template: list[
        GroupTemplateSchema
    ] = await group_template_service.remove_group_template_by_schema(
        obj_in=list_group_templates
    )
    return deleted_groups_template
