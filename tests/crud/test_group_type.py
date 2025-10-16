import pytest
import pytest_asyncio
from crud.crud_group_type import crud_group_type
from models.model_group_type import GroupTypeModel
from schemas.schema_group_type import GroupTypeSchemaBase
from sqlalchemy.ext.asyncio import AsyncSession
from store.db.base import Base

from tests.utils.utils import random_lower_string


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(async_session: AsyncSession):
    yield
    await async_session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await async_session.execute(table.delete())
    await async_session.commit()


class TestGroupType:
    SQLITE_CONSTRAINT_FOREIGNKEY_ERROR_CODE = 787
    SQLITE_CONSTRAINT_UNIQUE_ERROR_CODE = 2067
    SQLITE_CONSTRAINT_UNIQUE_ERROR_TEXT = (
        "UNIQUE constraint failed: group_type.name"
    )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_group_type(self, async_session: AsyncSession):
        group_type_base: list[GroupTypeSchemaBase] = [
            GroupTypeSchemaBase(name=random_lower_string(32)),
            GroupTypeSchemaBase(name=random_lower_string(32)),
        ]

        new_group_type: list[
            GroupTypeModel
        ] = await crud_group_type.create_group_type(
            session=async_session, obj_in=group_type_base
        )

        for idx in range(len(new_group_type)):
            assert group_type_base[idx].name == new_group_type[idx].name
            assert new_group_type[idx].id > 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_duplicate_create_group_type(
        self, async_session: AsyncSession
    ):
        group_type_name = random_lower_string(32)
        group_type_scheme: GroupTypeSchemaBase = GroupTypeSchemaBase(
            name=group_type_name
        )

        with pytest.raises(ValueError) as exc_info:
            await crud_group_type.create_group_type(
                session=async_session,
                obj_in=[group_type_scheme, group_type_scheme],
            )
        assert (
            exc_info.value.args[0].split(", ")[0]
            == self.SQLITE_CONSTRAINT_UNIQUE_ERROR_TEXT
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_empty_name_create_group_type(
        self, async_session: AsyncSession
    ):
        group_type_name = ""
        group_type_scheme: GroupTypeSchemaBase = GroupTypeSchemaBase(
            name=group_type_name
        )

        with pytest.raises(ValueError) as exc_info:
            await crud_group_type.create_group_type(
                session=async_session,
                obj_in=[group_type_scheme, group_type_scheme],
            )

        assert (
            exc_info.value.args[0].split(", ")[0]
            == self.SQLITE_CONSTRAINT_UNIQUE_ERROR_TEXT
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_type_by_id(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
    ):
        group_type: GroupTypeModel = await crud_group_type.get_group_type(
            session=async_session, data=predefined_group_type[0].id
        )

        assert group_type.name == predefined_group_type[0].name
        assert group_type.id == predefined_group_type[0].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_type_by_name(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
    ):
        group_type: GroupTypeModel = await crud_group_type.get_group_type(
            session=async_session, data=predefined_group_type[0].name
        )

        assert group_type.name == predefined_group_type[0].name
        assert group_type.id == predefined_group_type[0].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_type_wrong_parameters(
        self, async_session: AsyncSession
    ):
        wrong_data_parameter = 17.2

        with pytest.raises(TypeError) as exc_info:
            await crud_group_type.get_group_type(
                session=async_session, data=wrong_data_parameter
            )

        assert exc_info.type is TypeError

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(session=async_session)
        for idx in range(len(predefined_group_type_10)):
            assert (
                predefined_group_type_10[idx].name == all_group_types[idx].name
            )
            assert predefined_group_type_10[idx].id == all_group_types[idx].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_limit(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        limit = 2

        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(
            session=async_session, limit=limit
        )

        for idx in range(limit):
            assert (
                predefined_group_type_10[idx].name == all_group_types[idx].name
            )
            assert predefined_group_type_10[idx].id == all_group_types[idx].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_more_limit(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        limit = 100

        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(
            session=async_session, limit=limit
        )

        for idx in range(len(all_group_types)):
            assert (
                predefined_group_type_10[idx].name == all_group_types[idx].name
            )
            assert predefined_group_type_10[idx].id == all_group_types[idx].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_offset(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        offset = 2

        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(
            session=async_session, offset=offset
        )

        for idx in range(len(all_group_types)):
            assert (
                predefined_group_type_10[offset + idx].name
                == all_group_types[idx].name
            )
            assert (
                predefined_group_type_10[offset + idx].id
                == all_group_types[idx].id
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_more_offset(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        offset = len(predefined_group_type_10) + 1

        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(
            session=async_session, offset=offset
        )

        assert len(all_group_types) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_limit_and_offset(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        limit = 3
        offset = 2

        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(
            session=async_session, limit=limit, offset=offset
        )

        for idx in range(limit):
            assert (
                predefined_group_type_10[offset + idx].name
                == all_group_types[idx].name
            )
            assert (
                predefined_group_type_10[offset + idx].id
                == all_group_types[idx].id
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_limit_and_offset_twice(
        self,
        async_session: AsyncSession,
        predefined_group_type_10: list[GroupTypeModel],
    ):
        limit = 3
        offset = 2

        all_group_types = await crud_group_type.get_all_group_type(
            session=async_session, limit=limit, offset=offset
        )

        for idx in range(limit):
            assert (
                predefined_group_type_10[offset + idx].name
                == all_group_types[idx].name
            )
            assert (
                predefined_group_type_10[offset + idx].id
                == all_group_types[idx].id
            )
        offset = offset + limit + 1
        all_group_types_new: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(
            session=async_session, limit=limit, offset=offset
        )
        for idx in range(limit):
            assert (
                predefined_group_type_10[offset + idx].name
                == all_group_types_new[idx].name
            )
            assert (
                predefined_group_type_10[offset + idx].id
                == all_group_types_new[idx].id
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_wrong_limit_type(
        self, async_session: AsyncSession
    ):
        limit = "a"

        with pytest.raises(ValueError) as exc_info:
            await crud_group_type.get_all_group_type(
                session=async_session, limit=limit
            )

        assert exc_info.type is ValueError

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_wrong_offset_type(
        self, async_session: AsyncSession
    ):
        offset = "a"

        with pytest.raises(ValueError) as exc_info:
            await crud_group_type.get_all_group_type(
                session=async_session, offset=offset
            )

        assert exc_info.type is ValueError

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_with_wrong_data_type(
        self, async_session: AsyncSession
    ):
        limit = offset = "a"

        with pytest.raises(ValueError) as exc_info:
            await crud_group_type.get_all_group_type(
                session=async_session, limit=limit, offset=offset
            )

        assert exc_info.type is ValueError

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_all_group_type_empty(self, async_session: AsyncSession):
        all_group_types: list[
            GroupTypeModel
        ] = await crud_group_type.get_all_group_type(session=async_session)

        assert len(all_group_types) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_type_id(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
    ):
        group_type_id: int = await crud_group_type.get_group_type_id(
            session=async_session, name=predefined_group_type[0].name
        )

        assert group_type_id == predefined_group_type[0].id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_group_type_id_wrong_data(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
    ):
        group_type_id: int = await crud_group_type.get_group_type_id(
            session=async_session, name="weird_group_name"
        )

        assert group_type_id == 0
