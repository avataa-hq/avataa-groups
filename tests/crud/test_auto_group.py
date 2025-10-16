from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio
from crud.crud_group_template import crud_group_template
from models.model_group_type import GroupTypeModel
from pydantic import ValidationError
from schemas.schema_group_template import GroupTemplateMain, GroupTemplateSchema
from sqlalchemy.ext.asyncio import AsyncSession
from store.db.base_class import Base

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


class TestAutoGroup:
    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "template_name, col_filters, ranges_obj, ident, min_qnt, tmo_id, expected",
        [
            (
                random_lower_string(length=32, invalid_count=5),
                [],
                {},
                [1],
                1,
                1,
                True,
            ),
            (random_lower_string(1), [], {}, [1], 1, 1, True),
            ("1", [], {}, [1], 1, 1, True),
            ("1", [], {}, [1], 1, 2_147_483_646, True),
            ("1", ["a"], {}, [1], 1, 1, True),
            ("1", [], {1: 1}, [], 1, 1, True),
            ("1", [], {}, [1], 1, 1, True),
        ],
    )
    async def test_create_auto_group_success(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
        template_name: str,
        col_filters: list[Any],
        ranges_obj: dict | None,
        ident: list[int] | None,
        min_qnt: int,
        tmo_id: int,
        expected: bool,
    ) -> None:
        """Create auto group"""
        temp_group_template = GroupTemplateMain(
            name=template_name,
            column_filters=col_filters,
            ranges_object=ranges_obj,
            identical=ident,
            min_qnt=min_qnt,
            tmo_id=tmo_id,
            group_type_id_for_template=predefined_group_type[0].id,
        )
        result: list[
            GroupTemplateSchema
        ] = await crud_group_template.create_group_templates(
            session=async_session, obj_in=[temp_group_template]
        )
        assert len(result) == 1
        assert result[0].name == template_name

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "template_name, col_filters, ranges_obj, ident, min_qnt, tmo_id, expected",
        [
            ("", [], {}, [], 0, 1, ValidationError),
            (random_lower_string(260), [], {}, [], 0, 1, ValidationError),
            (" ", [], {}, [], 0, 1, ValidationError),
            (" " * 5, [], {}, [], 0, 1, ValidationError),
            ("1", [], {}, [], 0, -1, ValidationError),
            ("1", [], {}, [], 0, 0, ValidationError),
            ("1", [], {}, [], 0, "a", ValidationError),
            ("1", [], {}, [], 0, 2_147_483_647, ValidationError),
            ("1", [], {}, [], 2_147_483_647, 1, ValidationError),
            ("1", [], {}, [], -1, 1, ValidationError),
            ("1", {}, {}, [], 1, 1, ValidationError),
            ("1", "a", {}, [], 1, 1, ValidationError),
            ("1", 1, {}, [], 1, 1, ValidationError),
            ("1", [], "a", [], 1, 1, ValidationError),
            ("1", [], 1, [], 1, 1, ValidationError),
            ("1", [], {}, "a", 1, 1, ValidationError),
            ("1", [], {}, 1, 1, 1, ValidationError),
            ("1", [], {}, [], 1, 1, ValidationError),  # All filters are empty
        ],
    )
    async def test_create_auto_group_schema_failed(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
        template_name: str,
        col_filters: list[Any],
        ranges_obj: dict | None,
        ident: list[int] | None,
        min_qnt: int,
        tmo_id: int,
        expected: type[Exception],
    ) -> None:
        """Create auto group schema validation failed"""
        with pytest.raises(expected):
            GroupTemplateMain(
                name=template_name,
                column_filters=col_filters,
                ranges_object=ranges_obj,
                identical=ident,
                min_qnt=min_qnt,
                tmo_id=tmo_id,
                group_type_id_for_template=predefined_group_type[0].id,
            )

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "template_name, col_filters, ranges_obj, ident, min_qnt, tmo_id, expected, expected_message",
        [
            (
                "1",
                [],
                {},
                [1],
                0,
                1,
                ValueError,
                "duplicate key value violates unique constraint",
            ),
        ],
    )
    async def test_create_auto_group_crud_failed(
        self,
        async_session: AsyncSession,
        predefined_group_type: list[GroupTypeModel],
        template_name: str,
        col_filters: list[Any],
        ranges_obj: dict | None,
        ident: list[int] | None,
        min_qnt: int,
        tmo_id: int,
        expected: type[Exception],
        expected_message: str,
    ) -> None:
        """Create auto group crud failed"""
        temp_group_template = GroupTemplateMain(
            name=template_name,
            column_filters=col_filters,
            ranges_object=ranges_obj,
            identical=ident,
            min_qnt=min_qnt,
            tmo_id=tmo_id,
            group_type_id_for_template=predefined_group_type[0].id,
        )

        await crud_group_template.create_group_templates(
            session=async_session, obj_in=[temp_group_template]
        )
        with pytest.raises(expected) as exc_info:
            await crud_group_template.create_group_templates(
                session=async_session, obj_in=[temp_group_template]
            )
        assert expected_message in str(exc_info.value.args[0])

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_empty_auto_group(
        self, async_session: AsyncSession
    ) -> None:
        pass
