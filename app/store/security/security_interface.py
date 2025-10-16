from abc import ABC, abstractmethod

from starlette.requests import Request

from store.security.security_data_models import UserData


class SecurityInterface(ABC):
    @abstractmethod
    async def __call__(self, request: Request) -> UserData:
        # raise HTTPException if not authorized or not allowed
        pass
