from typing import Sequence

import pytest
import pytest_asyncio
from crud.crud_element import crud_element
from models.model_element import ElementModel
from models.model_group import GroupModel
from schemas.schema_element import ElementReadyToDB
from sqlalchemy.ext.asyncio import AsyncSession
from store.db.base import Base


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(async_session: AsyncSession):
    yield
    await async_session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await async_session.execute(table.delete())
    await async_session.commit()


class TestElement:
    SQLITE_CONSTRAINT_UNIQUE_ERROR_CODE = 2067
    PG_CONSTRAINT_UNIQUE_ERROR_CODE = "23505"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_element(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ):
        element_entity_id_1 = 5
        element_entity_id_2 = 7
        element_list: list[ElementReadyToDB] = [
            ElementReadyToDB(
                entity_id=element_entity_id_1,
                group_id=predefined_group[0].id,
            ),
            ElementReadyToDB(
                entity_id=element_entity_id_2,
                group_id=predefined_group[1].id,
            ),
        ]

        new_list_elements: list[
            ElementModel
        ] = await crud_element.create_element(
            session=async_session, obj_in=element_list
        )

        assert new_list_elements[0].id == 1
        assert new_list_elements[0].entity_id == element_entity_id_1
        assert new_list_elements[0].group_id == predefined_group[0].id
        assert new_list_elements[1].id == 2
        assert new_list_elements[1].entity_id == element_entity_id_2
        assert new_list_elements[1].group_id == predefined_group[1].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_element_duplicate(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ):
        element_entity_id_1 = element_entity_id_2 = 5
        element_list: list[ElementReadyToDB] = [
            ElementReadyToDB(
                entity_id=element_entity_id_1, group_id=predefined_group[0].id
            ),
            ElementReadyToDB(
                entity_id=element_entity_id_2, group_id=predefined_group[0].id
            ),
        ]

        with pytest.raises(ValueError) as exc_info:
            await crud_element.create_element(
                session=async_session, obj_in=element_list
            )

        assert (
            exc_info.value.args[2].pgcode
            == self.PG_CONSTRAINT_UNIQUE_ERROR_CODE
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_element_only_entity_duplicate(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ):
        element_entity_id_1 = element_entity_id_2 = 5
        element_list: list[ElementReadyToDB] = [
            ElementReadyToDB(
                entity_id=element_entity_id_1,
                group_id=predefined_group[0].id,
            ),
            ElementReadyToDB(
                entity_id=element_entity_id_2,
                group_id=predefined_group[1].id,
            ),
        ]

        new_list_elements: Sequence[
            ElementModel
        ] = await crud_element.create_element(
            session=async_session, obj_in=element_list
        )

        assert new_list_elements[0].entity_id == element_entity_id_1
        assert new_list_elements[0].group_id == predefined_group[0].id
        assert new_list_elements[1].entity_id == element_entity_id_2
        assert new_list_elements[1].group_id == predefined_group[1].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_select_element(self, async_session: AsyncSession):
        pass
