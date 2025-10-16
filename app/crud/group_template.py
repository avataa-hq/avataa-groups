from logging import getLogger

from schemas.schema_group_template import GroupTemplateSchema
from sqlalchemy.ext.asyncio import AsyncSession

from crud.crud_group_template import CRUDGroupTemplate


class GroupTemplateService(object):
    def __init__(
        self,
        repo_group_template: CRUDGroupTemplate,
        session: AsyncSession,
        lifespan_app,
    ) -> None:
        self.group_template_repo = repo_group_template
        self.session = session
        self.app = lifespan_app
        self.logger = getLogger("Group Service")

    async def get_group_template_by_ids(
        self, template_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        try:
            list_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.get_group_template_by_ids(
                session=self.session, template_ids=template_ids
            )
            return list_group_template
        except Exception as ex:
            self.logger.exception(ex)
            return []

    async def get_group_template_by_tmo_ids(
        self, tmo_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        try:
            list_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.get_group_template_by_tmo_ids(
                session=self.session, tmo_ids=tmo_ids
            )
            return list_group_template
        except Exception as ex:
            self.logger.exception(ex)
            return []

    async def remove_group_template_by_ids(
        self, template_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        try:
            list_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.get_group_template_by_ids(
                session=self.session, template_ids=template_ids
            )
            if not list_group_template:
                return []
            deleted_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.delete_group_template(
                session=self.session, obj_in=list_group_template
            )
            return deleted_group_template
        except Exception as ex:
            self.logger.exception(ex)
            return []

    async def remove_group_template_by_schema(
        self, obj_in: list[GroupTemplateSchema]
    ) -> list[GroupTemplateSchema]:
        try:
            deleted_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.delete_group_template(
                session=self.session, obj_in=obj_in
            )
            return deleted_group_template
        except Exception as ex:
            self.logger.exception(ex)
            return []

    async def remove_group_template_by_tmo_ids(
        self, tmo_ids: list[int]
    ) -> list[GroupTemplateSchema]:
        try:
            list_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.get_group_template_by_tmo_ids(
                session=self.session, tmo_ids=tmo_ids
            )
            if not list_group_template:
                return []
            deleted_group_template: list[
                GroupTemplateSchema
            ] = await self.group_template_repo.delete_group_template(
                session=self.session, obj_in=list_group_template
            )
            return deleted_group_template
        except Exception as ex:
            self.logger.exception(ex)
            return []
