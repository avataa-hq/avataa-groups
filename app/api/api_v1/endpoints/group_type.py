from typing import Annotated, Sequence

from crud.crud_group_type import crud_group_type
from fastapi import APIRouter, Depends, HTTPException, status
from models.model_group_type import GroupTypeModel
from schemas.schema_group_type import GroupTypeSchema
from sqlalchemy.ext.asyncio import AsyncSession

from api import utils

router = APIRouter()


class GetPagingParams:
    def __init__(self, offset: int = 0, limit: int = 15):
        self.offset = offset
        self.limit = limit


@router.get("/all", response_model=list[GroupTypeSchema])
async def get_all_group_type(
    *,
    session: AsyncSession = Depends(utils.get_session),
    paging: Annotated[GetPagingParams, Depends()],
):
    group: Sequence[GroupTypeModel] = await crud_group_type.get_all_group_type(
        session=session, limit=paging.limit, offset=paging.offset
    )
    if group:
        return group
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
    )


@router.get("/{group_type_id}", response_model=GroupTypeSchema)
async def get_group_type(
    *, session: AsyncSession = Depends(utils.get_session), group_type_id: int
):
    group: GroupTypeModel | None = await crud_group_type.get_group_type(
        session=session, data=group_type_id
    )
    if group:
        return group
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Group not found."
    )
