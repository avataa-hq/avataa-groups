from logging import getLogger
from typing import Sequence, Union

from models.model_group_template import GroupTemplateModel
from schemas.schema_group_template import GroupTemplateMain, GroupTemplateSchema
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import subqueryload

from crud.protocols.utils import handle_db_exceptions

_U = Union[int, str]


class CRUDGroupTemplate:
    def __init__(self):
        self.logger = getLogger("CRUD group template")

    @handle_db_exceptions
    async def create_group_templates(
        self, session: AsyncSession, obj_in: list[GroupTemplateMain]
    ) -> list[GroupTemplateSchema]:
        stmt = insert(GroupTemplateModel).returning(GroupTemplateModel)
        params_entity = [el.model_dump() for el in obj_in]
        result: Sequence[GroupTemplateModel] = (
            await session.scalars(stmt, params_entity)
        ).all()
        await session.commit()
        return [res.to_schema() for res in result]

    async def get_group_template(
        self, session: AsyncSession, data: _U
    ) -> GroupTemplateModel | None:
        if isinstance(data, int):
            stmt = select(GroupTemplateModel).where(
                GroupTemplateModel.id == data
            )
        elif isinstance(data, str):
            stmt = select(GroupTemplateModel).where(
                GroupTemplateModel.name == data
            )
        else:
            self.logger.error("Wrong group template.")
            raise TypeError("Wrong Group template")
        group_type: GroupTemplateModel | None = await session.scalar(
            statement=stmt
        )
        return group_type

    async def get_group_template_by_ids(
        self, session: AsyncSession, template_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        stmt = (
            select(GroupTemplateModel)
            .where(GroupTemplateModel.id.in_(template_ids))
            .options(subqueryload(GroupTemplateModel.groups_for_template))
        )
        group_templates: Sequence[GroupTemplateModel] = (
            await session.scalars(statement=stmt)
        ).all()
        self.logger.debug("Get information about group template ids.")
        return [res for res in group_templates]

    async def get_group_template_by_tmo_ids(
        self, session: AsyncSession, tmo_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        stmt = (
            select(GroupTemplateModel)
            .where(GroupTemplateModel.tmo_id.in_(tmo_ids))
            .options(subqueryload(GroupTemplateModel.groups_for_template))
        )
        group_templates: Sequence[GroupTemplateModel] = (
            await session.scalars(statement=stmt)
        ).all()
        self.logger.debug("Get information about group template ids.")
        return [res.to_schema() for res in group_templates]

    async def get_all_group_template(
        self, session: AsyncSession, limit=15, offset=0
    ) -> list[GroupTemplateModel]:
        try:
            stmt = select(GroupTemplateModel).offset(offset).limit(limit)
            result: Sequence[GroupTemplateModel] = (
                await session.scalars(statement=stmt)
            ).all()
            return list(result)
        except InvalidRequestError as ex:
            self.logger.exception("%s", ex)
            raise ValueError(f"Can't get all group template {ex.args[0]}")

    async def delete_group_template(
        self, session: AsyncSession, obj_in: list[GroupTemplateSchema]
    ) -> list[GroupTemplateSchema]:
        stmt = (
            delete(GroupTemplateModel)
            .where(
                GroupTemplateModel.id.in_(
                    [gr_template.id for gr_template in obj_in]
                )
            )
            .returning(GroupTemplateModel)
        )
        self.logger.debug(
            f"Removed group templates with id {[gr_template.id for gr_template in obj_in]}"
        )
        result: Sequence[GroupTemplateModel] = (
            await session.scalars(statement=stmt)
        ).all()
        await session.commit()
        return [res.to_schema() for res in result]

    async def delete_by_tmo_id(
        self, session: AsyncSession, tmo_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        stmt = (
            delete(GroupTemplateModel)
            .where(GroupTemplateModel.tmo_id.in_(tmo_ids))
            .returning(GroupTemplateModel)
        )
        self.logger.debug("Removed group templates with tmo ids %s", tmo_ids)
        result: Sequence[GroupTemplateModel] = (
            await session.scalars(statement=stmt)
        ).all()
        await session.commit()
        return [gr_template.to_schema() for gr_template in result]


crud_group_template = CRUDGroupTemplate()
