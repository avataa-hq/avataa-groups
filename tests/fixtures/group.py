from typing import AsyncGenerator

import pytest_asyncio
from models.model_group import GroupModel
from models.model_group_type import GroupTypeModel
from sqlalchemy.ext.asyncio import AsyncSession


@pytest_asyncio.fixture(loop_scope="session")
async def predefined_group(
    async_session: AsyncSession, predefined_group_type: list[GroupTypeModel]
) -> AsyncGenerator[list[GroupModel], None]:
    group_1_name = "Group_1"
    group_2_name = "Group_2"
    process_instance_key_1 = 1
    process_instance_key_2 = 2
    group_model_list: list[GroupModel] = [
        GroupModel(
            group_name=group_1_name,
            group_process_instance_key=process_instance_key_1,
            group_type_id=predefined_group_type[0].id,
            tmo_id=1,
            is_valid=True,
            column_filters=[],
            id=1,
            group_type=predefined_group_type[0],
            elements=[],
            ranges_object={},
            group_template_id=None,
            group_template=None,
            min_qnt=None,
        ),
        GroupModel(
            group_name=group_2_name,
            group_process_instance_key=process_instance_key_2,
            group_type_id=predefined_group_type[1].id,
            tmo_id=1,
            is_valid=True,
            column_filters=[],
            id=2,
            group_type=predefined_group_type[0],
            elements=[],
            ranges_object={},
            group_template_id=None,
            group_template=None,
            min_qnt=None,
        ),
    ]
    async_session.add_all(group_model_list)
    await async_session.flush()

    yield group_model_list
    await async_session.rollback()
