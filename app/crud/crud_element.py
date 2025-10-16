from typing import Sequence

from models.model_element import ElementModel
from schemas.schema_element import ElementReadyToDB, ElementSchema
from sqlalchemy import and_, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import subqueryload


class CRUDElement:
    """Repository"""

    @staticmethod
    async def create_element(
        session: AsyncSession, obj_in: list[ElementReadyToDB]
    ) -> Sequence[ElementModel]:
        stmt = insert(ElementModel).returning(ElementModel)
        params_entity = [el.model_dump() for el in obj_in]
        try:
            result: Sequence[ElementModel] = (
                await session.scalars(stmt, params_entity)
            ).all()
            await session.commit()
            return result
        except IntegrityError as ex:
            raise ValueError("Unique constraint violated", ex.params, ex.orig)
        except Exception as ex:
            raise RuntimeError(f"{type(ex)}: {ex}.")

    @staticmethod
    async def create_element_schema(
        session: AsyncSession, obj_in: list[ElementReadyToDB]
    ) -> list[ElementSchema]:
        stmt = insert(ElementModel).returning(ElementModel)
        params_entity = [el.model_dump() for el in obj_in]
        try:
            result: Sequence[ElementModel] = (
                await session.scalars(stmt, params_entity)
            ).all()
            await session.commit()
            return [res.to_schema() for res in result]
        except IntegrityError as ex:
            raise RuntimeError(
                f"orig={ex.orig}, params={ex.params}, statement={ex.statement}"
            )
        except Exception as ex:
            raise RuntimeError(f"{type(ex)}: {ex}.")

    @staticmethod
    async def select_elements(
        session: AsyncSession, elements: list[int]
    ) -> Sequence[ElementModel]:
        stmt = select(ElementModel).where(ElementModel.entity_id.in_(elements))
        result: Sequence[ElementModel] = (await session.scalars(stmt)).all()
        return result

    @staticmethod
    async def select_by_group_id(
        session: AsyncSession, group_id: int
    ) -> Sequence[ElementModel]:
        stmt = select(ElementModel).where(ElementModel.group_id == group_id)
        result: Sequence[ElementModel] = (await session.scalars(stmt)).all()
        return result

    @staticmethod
    async def select_by_group_id_schema(
        session: AsyncSession, group_id: int
    ) -> list[ElementSchema]:
        stmt = select(ElementModel).where(ElementModel.group_id == group_id)
        result: Sequence[ElementModel] = (await session.scalars(stmt)).all()
        return [res.to_schema() for res in result]

    @staticmethod
    async def select_by_group_id_schema_with_update(
        session: AsyncSession, group_id: int
    ) -> list[ElementSchema]:
        stmt = (
            select(ElementModel)
            .where(ElementModel.group_id == group_id)
            .with_for_update()
        )
        result: Sequence[ElementModel] = (await session.scalars(stmt)).all()
        return [res.to_schema() for res in result]

    @staticmethod
    async def select_all_entity_id(
        session: AsyncSession,
    ) -> Sequence[int] | None:
        stmt = select(ElementModel.entity_id)
        result: Sequence[int] = (await session.scalars(stmt)).all()
        return result

    @staticmethod
    async def select_group_name(
        session: AsyncSession, obj_in: int
    ) -> list[str]:
        stmt = (
            select(ElementModel)
            .where(ElementModel.entity_id == obj_in)
            .options(subqueryload(ElementModel.group))
        )
        result: Sequence[ElementModel] = (await session.scalars(stmt)).all()
        group_names: list[str] = []
        for row in result:
            group_names.append(row.group.group_name)
        return group_names

    @staticmethod
    async def select_group_names(
        session: AsyncSession, obj_in: list[int]
    ) -> list[str]:
        stmt = (
            select(ElementModel)
            .where(ElementModel.entity_id.in_(obj_in))
            .options(subqueryload(ElementModel.group))
        )
        result: Sequence[ElementModel] = (await session.scalars(stmt)).all()
        group_names: list[str] = []
        for row in result:
            group_names.append(row.group.group_name)
        return group_names

    @staticmethod
    async def update(
        session: AsyncSession, obj_in: list[ElementModel], group_id: int
    ) -> list[ElementModel]:
        for model in obj_in:
            model.group_id = group_id
        await session.commit()
        return obj_in

    @staticmethod
    async def delete(
        session: AsyncSession, group_id: int, obj_in: list[ElementReadyToDB]
    ) -> Sequence[ElementModel]:
        stmt = select(ElementModel).where(
            and_(
                ElementModel.entity_id.in_([el.entity_id for el in obj_in]),
                ElementModel.group_id == group_id,
            )
        )
        removed_elements: Sequence[ElementModel] = (
            await session.scalars(stmt)
        ).all()
        for element in removed_elements:
            await session.delete(element)
        await session.commit()
        return removed_elements


crud_element = CRUDElement()
