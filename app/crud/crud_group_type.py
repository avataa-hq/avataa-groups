from logging import getLogger
from typing import Sequence, Union

from models.model_group_type import GroupTypeModel
from schemas.schema_group_type import GroupTypeSchemaBase
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

_U = Union[int, str]


class CRUDGroupType:
    def __init__(self):
        self.logger = getLogger("CRUD group type")

    async def create_group_type(
        self, session: AsyncSession, obj_in: list[GroupTypeSchemaBase]
    ) -> Sequence[GroupTypeModel]:
        stmt = insert(GroupTypeModel).returning(GroupTypeModel)
        params_entity = [el.model_dump() for el in obj_in]
        try:
            result: Sequence[GroupTypeModel] = (
                await session.scalars(stmt, params_entity)
            ).all()
            await session.commit()
            return result
        except IntegrityError as ex:
            await session.rollback()
            self.logger.exception(ex)
            raise ValueError(
                "UNIQUE constraint failed: group_type.name", ex.params, ex.orig
            )
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    @staticmethod
    async def get_group_type(
        session: AsyncSession, data: _U
    ) -> GroupTypeModel | None:
        if isinstance(data, int):
            stmt = select(GroupTypeModel).where(GroupTypeModel.id == data)
        elif isinstance(data, str):
            stmt = select(GroupTypeModel).where(GroupTypeModel.name == data)
        else:
            raise TypeError("Wrong Group Type type")
        group_type: GroupTypeModel | None = await session.scalar(statement=stmt)
        return group_type

    @staticmethod
    async def get_all_group_type(
        session: AsyncSession, limit: int = 15, offset: int = 0
    ) -> list[GroupTypeModel]:
        try:
            stmt = select(GroupTypeModel).offset(offset).limit(limit)
            result: Sequence[GroupTypeModel] = (
                await session.scalars(statement=stmt)
            ).all()
            return list(result)
        except ValueError:
            raise ValueError("Wrong data")

    @staticmethod
    async def get_group_type_id(session: AsyncSession, name: str) -> int:
        stmt = select(GroupTypeModel.id).where(GroupTypeModel.name == name)
        result: int | None = await session.scalar(statement=stmt)
        if result:
            return result
        else:
            return 0


crud_group_type = CRUDGroupType()
