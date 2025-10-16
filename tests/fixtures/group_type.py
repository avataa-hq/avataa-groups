from typing import AsyncGenerator

import pytest_asyncio
from models.model_group_type import GroupTypeModel
from sqlalchemy.ext.asyncio import AsyncSession


@pytest_asyncio.fixture(loop_scope="session")
async def predefined_group_type(
    async_session: AsyncSession,
) -> AsyncGenerator[list[GroupTypeModel], None]:
    group_type_1_name = "group_type_1"
    group_type_2_name = "group_type_2"

    group_type_model_1 = GroupTypeModel()
    group_type_model_1.name = group_type_1_name
    group_type_model_2 = GroupTypeModel()
    group_type_model_2.name = group_type_2_name

    group_type_models: list[GroupTypeModel] = [
        group_type_model_1,
        group_type_model_2,
    ]
    async_session.add_all(group_type_models)

    await async_session.flush()
    yield group_type_models

    await async_session.rollback()


@pytest_asyncio.fixture(loop_scope="session")
async def predefined_group_type_10(
    async_session: AsyncSession,
) -> AsyncGenerator[list[GroupTypeModel], None]:
    list_names: list[str] = []
    for idx in range(10):
        list_names.append("group_type_" + str(idx))

    group_type_models: list[GroupTypeModel] = []
    for cur_name in list_names:
        temp_model = GroupTypeModel()
        temp_model.name = cur_name
        group_type_models.append(temp_model)

    async_session.add_all(group_type_models)

    await async_session.flush()
    yield group_type_models

    await async_session.rollback()
