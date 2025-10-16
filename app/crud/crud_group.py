from logging import getLogger
from typing import Sequence, Union

from models.model_element import ElementModel
from models.model_group import GroupModel
from schemas.schema_group import GroupBase, GroupSchema
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, InvalidRequestError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload, subqueryload

_U = Union[int, str]
_U_list = Union[list[int], list[str]]


class CRUDGroup:
    def __init__(self):
        self.logger = getLogger("CRUD group")
        self.wrong_data_error = "Wrong data"

    async def create_groups(
        self, session: AsyncSession, obj_in: list[GroupBase]
    ) -> Sequence[GroupModel]:
        stmt = (
            insert(GroupModel)
            .options(selectinload(GroupModel.group_type))
            .returning(GroupModel)
        )
        params_entity = [el.model_dump(exclude_none=True) for el in obj_in]
        try:
            result: Sequence[GroupModel] = (
                await session.scalars(stmt, params_entity)
            ).all()
            await session.commit()
            return result
        except IntegrityError as ex:
            await session.rollback()
            self.logger.exception(ex)
            raise ValueError(
                "UNIQUE constraint failed: group.group_name", ex.params, ex.orig
            )
        except Exception as ex:
            await session.rollback()
            raise ConnectionError(f"{type(ex)}: {ex}")

    async def create_groups_schema(
        self, session: AsyncSession, obj_in: list[GroupBase]
    ) -> list[GroupSchema]:
        stmt = (
            insert(GroupModel)
            .options(selectinload(GroupModel.group_type))
            .returning(GroupModel)
        )
        params_entity = [el.model_dump(exclude_none=True) for el in obj_in]
        try:
            result: Sequence[GroupModel] = (
                await session.scalars(stmt, params_entity)
            ).all()
            await session.commit()
            return [res.to_schema() for res in result]
        except IntegrityError as ex:
            await session.rollback()
            self.logger.exception(ex)
            raise ValueError(f"{ex.orig}, {ex.params}, {ex.statement}")
        except Exception as ex:
            await session.rollback()
            raise ConnectionError(f"{type(ex)}: {ex}")

    async def get_group(
        self, session: AsyncSession, data: _U
    ) -> GroupModel | None:
        if isinstance(data, int):
            stmt = (
                select(GroupModel)
                .where(GroupModel.id == data)
                .options(
                    subqueryload(GroupModel.group_type),
                    subqueryload(GroupModel.elements),
                )
            )
        elif isinstance(data, str):
            stmt = (
                select(GroupModel)
                .where(GroupModel.group_name == data)
                .options(
                    subqueryload(GroupModel.group_type),
                    subqueryload(GroupModel.elements),
                )
            )
        else:
            raise TypeError(self.wrong_data_error)
        result: GroupModel | None = await session.scalar(statement=stmt)
        return result

    async def get_group_schema(
        self, session: AsyncSession, data: _U
    ) -> GroupSchema | None:
        if isinstance(data, int):
            stmt = (
                select(GroupModel)
                .where(GroupModel.id == data)
                .options(
                    subqueryload(GroupModel.group_type),
                    subqueryload(GroupModel.elements),
                )
            )
        elif isinstance(data, str):
            stmt = (
                select(GroupModel)
                .where(GroupModel.group_name == data)
                .options(
                    subqueryload(GroupModel.group_type),
                    subqueryload(GroupModel.elements),
                )
            )
        else:
            raise TypeError(self.wrong_data_error)
        result: GroupModel | None = await session.scalar(statement=stmt)
        if result:
            return result.to_schema()
        return None

    async def get_group_with_elements(
        self, session: AsyncSession, data: _U
    ) -> GroupModel | None:
        if isinstance(data, int):
            stmt = (
                select(GroupModel)
                .where(GroupModel.id == data)
                .options(
                    subqueryload(GroupModel.elements),
                    subqueryload(GroupModel.group_type),
                )
            )
        elif isinstance(data, str):
            stmt = (
                select(GroupModel)
                .where(GroupModel.group_name == data)
                .options(
                    subqueryload(GroupModel.elements),
                    subqueryload(GroupModel.group_type),
                )
            )
        else:
            raise TypeError(self.wrong_data_error)
        result: GroupModel | None = await session.scalar(statement=stmt)
        return result

    @staticmethod
    async def get_group_with_elements_by_tmo_id(
        session: AsyncSession, tmo_ids: list[int]
    ) -> list[GroupModel]:
        stmt = (
            select(GroupModel)
            .where(GroupModel.tmo_id.in_(tmo_ids))
            .options(
                subqueryload(GroupModel.elements),
                subqueryload(GroupModel.group_type),
            )
        )
        result: Sequence[GroupModel] = (
            await session.scalars(statement=stmt)
        ).all()
        return list(result)

    @staticmethod
    async def get_all_group(
        session: AsyncSession, limit: int = 15, offset: int = 0
    ) -> list[GroupModel]:
        try:
            stmt = select(GroupModel).offset(offset).limit(limit)
            result: Sequence[GroupModel] = (
                await session.scalars(statement=stmt)
            ).all()
            return list(result)
        except TimeoutError:
            raise RuntimeError("Unable to connect to the database")
        except InvalidRequestError as ex:
            raise ValueError(f"Can't get all group {ex.args[0]}")
        except ProgrammingError as ex:
            raise ValueError(f"Check migration version {ex.args[0]}")
        except Exception as ex:
            raise ValueError(f"Can't get all group {ex.args[0]}")

    @staticmethod
    async def get_all_group_schema(
        session: AsyncSession, limit: int = 15, offset: int = 0
    ) -> list[GroupSchema]:
        try:
            stmt = select(GroupModel).offset(offset).limit(limit)
            result: Sequence[GroupModel] = (
                await session.scalars(statement=stmt)
            ).all()
            return [group.to_schema() for group in result]
        except TimeoutError:
            raise RuntimeError("Unable to connect to the database")
        except InvalidRequestError as ex:
            raise ValueError(f"Can't get all group {ex.args[0]}")
        except ProgrammingError as ex:
            raise ValueError(f"Check migration version {ex.args[0]}")
        except Exception as ex:
            raise ValueError(f"Can't get all group {ex.args[0]}")

    @staticmethod
    async def get_all_group_by_type(
        session: AsyncSession,
        group_type_id: int,
        limit: int = 15,
        offset: int = 0,
    ) -> Sequence[GroupModel]:
        stmt = (
            select(GroupModel)
            .where(GroupModel.group_type_id == group_type_id)
            .offset(offset)
            .limit(limit)
        )
        result: Sequence[GroupModel] = (
            await session.scalars(statement=stmt)
        ).all()
        return result

    @staticmethod
    async def update_group_process_id(
        session: AsyncSession, obj_in: GroupModel, process_id: int | None
    ) -> GroupModel:
        obj_in.group_process_instance_key = process_id
        await session.commit()
        await session.refresh(obj_in)
        return obj_in

    @staticmethod
    async def update_valid(
        session: AsyncSession, obj_in: GroupModel, is_valid: bool
    ) -> GroupModel:
        obj_in.is_valid = is_valid
        await session.commit()
        return obj_in

    @staticmethod
    async def update_valid_schema(
        session: AsyncSession, obj_in: GroupSchema, is_valid: bool
    ) -> GroupSchema | None:
        stmt = (
            update(GroupModel)
            .where(GroupModel.group_name == obj_in.group_name)
            .values(is_valid=obj_in.is_valid)
            .returning(GroupModel)
        )
        result: GroupModel | None = await session.scalar(statement=stmt)
        await session.commit()
        if result:
            return result.to_schema()
        return None

    async def remove_groups(
        self, session: AsyncSession, obj_in: list[GroupModel]
    ) -> list[GroupModel]:
        stmt = (
            delete(GroupModel)
            .where(GroupModel.id.in_([gr.id for gr in obj_in]))
            .returning(GroupModel)
        )
        try:
            result: Sequence[GroupModel] = (
                await session.scalars(statement=stmt)
            ).all()
            await session.commit()
            return result
        except IntegrityError as ex:
            await session.rollback()
            self.logger.exception(ex)
            return []
        except Exception as ex:
            await session.rollback()
            self.logger.exception(ex)
            # raise ConnectionError(f"{type(ex)}: {ex}")
            return []

    @staticmethod
    async def remove_by_tmo_id(
        session: AsyncSession, tmo_ids: list[int]
    ) -> list[GroupSchema]:
        stmt = (
            delete(GroupModel)
            .where(GroupModel.tmo_id.in_(tmo_ids))
            .returning(GroupModel)
        )
        result: Sequence[GroupModel] = (
            await session.scalars(statement=stmt)
        ).all()
        await session.commit()
        return [removed_gr.to_schema(delete=True) for removed_gr in result]

    @staticmethod
    async def remove_groups_by_schema(
        session: AsyncSession, obj_in: list[GroupSchema]
    ) -> list[GroupSchema]:
        stmt = (
            delete(GroupModel)
            .where(GroupModel.id.in_([gr.id for gr in obj_in]))
            .returning(GroupModel)
        )
        result: Sequence[GroupModel] = (
            await session.scalars(statement=stmt)
        ).all()
        output = [res.to_schema(delete=True) for res in result]
        await session.commit()
        return output

    @staticmethod
    async def get_not_null_filter(
        session: AsyncSession,
    ) -> Sequence[GroupModel]:
        stmt = (
            select(GroupModel)
            .where(GroupModel.column_filters.is_not(None))
            .options(joinedload(GroupModel.elements))
        )
        result: Sequence[GroupModel] = (
            (await session.scalars(statement=stmt)).unique().all()
        )
        return result

    @staticmethod
    async def get_group_names_by_tmo(
        session: AsyncSession, tmo_ids: list[int]
    ) -> list[str]:
        stmt = select(GroupModel.group_name).where(
            GroupModel.tmo_id.in_(tmo_ids)
        )
        result: Sequence[str] = (await session.scalars(statement=stmt)).all()
        return list(result)

    @staticmethod
    async def get_count_group(session: AsyncSession, group_type_id: int) -> int:
        stmt = (
            select(func.count())
            .select_from(GroupModel)
            .where(GroupModel.group_type_id == group_type_id)
        )
        result: int = (await session.scalars(statement=stmt)).first()
        return result

    @staticmethod
    async def get_group_schema_by_element_id(
        session: AsyncSession, obj_in: list[int]
    ) -> list[GroupSchema]:
        stmt = (
            select(GroupModel)
            .join(ElementModel, ElementModel.group_id == GroupModel.id)
            .where(ElementModel.entity_id.in_(obj_in))
            .options(joinedload(GroupModel.elements))
        )
        result: Sequence[GroupModel] = (
            (await session.scalars(statement=stmt)).unique().all()
        )
        return [group.to_schema() for group in result]


crud_group = CRUDGroup()
