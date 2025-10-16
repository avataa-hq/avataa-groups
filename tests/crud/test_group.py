from typing import AsyncGenerator, Sequence

import pytest
import pytest_asyncio
from crud.crud_group import crud_group
from models.model_group import GroupModel
from models.model_group_type import GroupTypeModel
from schemas.schema_group import GroupBase
from sqlalchemy.ext.asyncio import AsyncSession
from store.db.base import Base

from tests.utils.utils import random_lower_string


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(
    async_session: AsyncSession,
) -> AsyncGenerator[None, None]:
    yield
    await async_session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await async_session.execute(table.delete())
    await async_session.commit()


class TestGroup:
    SQLITE_CONSTRAINT_FOREIGNKEY_ERROR_CODE = 787
    SQLITE_CONSTRAINT_FOREIGNKEY_ERROR_TEXT = "FOREIGN KEY constraint failed"
    SQLITE_CONSTRAINT_UNIQUE_ERROR_CODE = 2067
    PG_CONSTRAINT_UNIQUE_ERROR_CODE = "23505"
    PG_CONSTRAINT_UNIQUE_FQ_ERROR_CODE = "23503"
    CONSTRAINT_UNIQUE_ERROR_TEXT = "UNIQUE constraint failed: group.group_name"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_group(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
    ) -> None:
        group_name = random_lower_string(32)
        group_scheme: GroupBase = GroupBase(
            group_name=group_name,
            group_type_id=predefined_group_type[0].id,
            tmo_id=1,
        )

        new_group: Sequence[GroupModel] = await crud_group.create_groups(
            session=async_session,
            obj_in=[group_scheme],
        )

        assert new_group[0].group_name == group_name
        assert new_group[0].group_type_id == predefined_group_type[0].id
        assert new_group[0].group_type.name == predefined_group_type[0].name

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_group_with_wrong_group_type(
        self, async_session: AsyncSession
    ) -> None:
        group_name = random_lower_string(32)
        group_scheme: GroupBase = GroupBase(
            group_name=group_name, group_type_id=1, tmo_id=1
        )

        with pytest.raises(ValueError) as exc_info:
            await crud_group.create_groups(
                session=async_session, obj_in=[group_scheme]
            )

        assert (
            exc_info.value.args[2].pgcode
            == self.PG_CONSTRAINT_UNIQUE_FQ_ERROR_CODE
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_duplicate_create_group(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
    ) -> None:
        group_name = random_lower_string(32)
        group_scheme: GroupBase = GroupBase(
            group_name=group_name, group_type_id=1, tmo_id=1
        )

        with pytest.raises(ValueError) as exc_info:
            await crud_group.create_groups(
                session=async_session,
                obj_in=[group_scheme, group_scheme],
            )

        assert (
            exc_info.value.args[0].split(", ")[0]
            == self.CONSTRAINT_UNIQUE_ERROR_TEXT
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_by_id(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        current_group: GroupModel = await crud_group.get_group(
            session=async_session, data=predefined_group[0].id
        )

        assert current_group.id == predefined_group[0].id
        assert current_group.group_type_id == predefined_group[0].group_type_id
        assert current_group.group_name == predefined_group[0].group_name
        assert current_group.group_type.id == predefined_group[0].group_type.id
        assert (
            current_group.group_type.name == predefined_group[0].group_type.name
        )
        assert current_group.elements == []

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_by_name(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        current_group: GroupModel = await crud_group.get_group(
            session=async_session, data=predefined_group[0].group_name
        )

        assert current_group.id == predefined_group[0].id
        assert current_group.group_type_id == predefined_group[0].group_type_id
        assert current_group.group_name == predefined_group[0].group_name
        assert current_group.group_type.id == predefined_group[0].group_type.id
        assert (
            current_group.group_type.name == predefined_group[0].group_type.name
        )
        assert current_group.elements == []

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_by_float(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        data_float = 17.2

        with pytest.raises(TypeError) as exc_info:
            await crud_group.get_group(session=async_session, data=data_float)

        assert exc_info.type is TypeError

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_by_wrong_id(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        wrong_id = -1

        current_group: GroupModel = await crud_group.get_group(
            session=async_session, data=wrong_id
        )

        assert current_group is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_by_wrong_name(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        wrong_name = random_lower_string(10)

        current_group: GroupModel = await crud_group.get_group(
            session=async_session, data=wrong_name
        )

        assert current_group is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_with_elements_by_id(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        current_group_with_elements: GroupModel = await crud_group.get_group(
            session=async_session, data=predefined_group[0].id
        )

        assert current_group_with_elements.elements == []

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_with_elements_by_name(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        current_group_with_elements: GroupModel = await crud_group.get_group(
            session=async_session, data=predefined_group[0].group_name
        )

        assert current_group_with_elements.elements == []

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_with_elements_by_float(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        data_float = 17.2

        with pytest.raises(TypeError) as exc_info:
            await crud_group.get_group(session=async_session, data=data_float)

        assert exc_info.type is TypeError

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_with_elements_by_wrong_name(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        wrong_name = random_lower_string(10)

        current_group: GroupModel = await crud_group.get_group(
            session=async_session, data=wrong_name
        )

        assert current_group is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_group(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        removed_group: GroupModel = await crud_group.remove_groups(
            session=async_session, obj_in=[predefined_group[0]]
        )

        assert removed_group[0].id == predefined_group[0].id
        assert (
            removed_group[0].group_type_id == predefined_group[0].group_type_id
        )
        assert removed_group[0].group_name == predefined_group[0].group_name

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_wrong_group(
        self, async_session: AsyncSession, predefined_group: list[GroupModel]
    ) -> None:
        fake_id = len(predefined_group) + 1
        fake_grop_name = random_lower_string(10)
        fake_group_model: GroupModel = GroupModel(
            id=fake_id,
            group_name=fake_grop_name,
            group_type_id=predefined_group[0].group_type_id,
            group_process_instance_key=1,
            group_type=predefined_group[0].group_type,
            tmo_id=1,
            column_filters=[],
            is_valid=True,
            elements=[],
            ranges_object={},
            group_template_id=None,
            group_template=None,
            min_qnt=None,
        )

        removed_group: GroupModel = await crud_group.remove_groups(
            session=async_session, obj_in=[fake_group_model]
        )

        assert removed_group == []
