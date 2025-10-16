from typing import AsyncGenerator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from store.security.security_data_models import UserData
from store.security.security_factory import security


async def get_session(
    request: Request,
    user_data: UserData = Depends(security),
) -> AsyncGenerator[AsyncSession, None]:
    async with request.state.lifespan_app.database.session() as session:
        yield session
