import asyncio
import logging
from typing import Dict, Optional

import jwt
from fastapi import HTTPException
from fastapi.requests import Request
from fastapi.security import OAuth2AuthorizationCodeBearer
from httpx import AsyncClient, ConnectError, InvalidURL, ResponseNotRead

from store.security.implementation.utils.user_info_cache import (
    UserInfoCacheInterface,
)
from store.security.security_data_models import UserData
from store.security.security_interface import SecurityInterface


class Keycloak(OAuth2AuthorizationCodeBearer, SecurityInterface):
    def __init__(
        self,
        keycloak_public_url: str,
        authorization_url: str,
        token_url: str,
        refresh_url: Optional[str] = None,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
        options: Optional[dict] = None,
    ):
        super(Keycloak, self).__init__(
            authorizationUrl=authorization_url,
            tokenUrl=token_url,
            refreshUrl=refresh_url,
            scheme_name=scheme_name,
            scopes=scopes,
            description=description,
            auto_error=auto_error,
        )
        self.keycloak_public_url = keycloak_public_url
        self._public_key = None
        if not options:
            options = {
                "verify_signature": True,
                "verify_aud": False,
                "verify_exp": True,
            }
        self._options = options
        self.EXCEPTION_ERROR = "Token verification service unavailable"

    async def _get_public_key(self):
        try:
            async with AsyncClient() as session:
                resp = await session.get(self.keycloak_public_url, timeout=5.0)
                if resp.status_code != 200:
                    raise HTTPException(
                        status_code=503,
                        detail=self.EXCEPTION_ERROR,
                    )
                data = resp.json()
        except ConnectError:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        except asyncio.TimeoutError:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        except ResponseNotRead:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        except InvalidURL:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)

        public_key = (
            "-----BEGIN PUBLIC KEY-----\n"
            + data["public_key"]
            + "\n-----END PUBLIC KEY-----"
        )
        return public_key

    async def __call__(self, request: Request) -> UserData:
        token = await super(Keycloak, self).__call__(request)
        user_info = await self._parse_jwt(token=token)
        return UserData.from_jwt(user_info)

    async def _parse_jwt(self, token: str) -> dict:
        if self._public_key is None:
            self._public_key = await self._get_public_key()

        user_info = await self._decode_token(token)
        return user_info

    async def _decode_token(self, token: str):
        try:
            decoded_token = jwt.decode(
                token,
                self._public_key,
                algorithms=["RS256"],
                options=self._options,
            )
        except jwt.PyJWTError as e:
            logging.warning(e)
            raise HTTPException(status_code=403, detail=str(e))
        return decoded_token


class KeycloakInfo(Keycloak):
    INFO_PREFIX = "/protocol/openid-connect/userinfo"

    def __init__(
        self,
        cache: UserInfoCacheInterface | None,
        keycloak_public_url: str,
        authorization_url: str,
        token_url: str,
        refresh_url: Optional[str] = None,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
        options: Optional[dict] = None,
        cache_user_info_url: str | None = None,
    ):
        super(KeycloakInfo, self).__init__(
            keycloak_public_url=keycloak_public_url,
            authorization_url=authorization_url,
            token_url=token_url,
            refresh_url=refresh_url,
            scheme_name=scheme_name,
            scopes=scopes,
            description=description,
            auto_error=auto_error,
            options=options,
        )
        self.info_url = (
            cache_user_info_url
            or f"{self.keycloak_public_url}{self.INFO_PREFIX}"
        )
        self.cache = cache

    async def __call__(self, request: Request) -> UserData:
        token = await super(Keycloak, self).__call__(request)
        user_info = await self._parse_jwt(token=token)
        additional_data = await self.get_user_info(token=token)
        if additional_data:
            user_info.update(additional_data)
        return UserData.from_jwt(user_info)

    async def get_from_cache(self, token: str) -> dict | None:
        if not self.cache:
            return None
        return self.cache.get(token)

    async def set_in_cache(self, token: str, value: dict) -> None:
        if not self.cache:
            return
        self.cache.set(token, value)

    async def get_from_keycloak(self, token: str) -> dict | None:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with AsyncClient() as session:
                resp = await session.get(
                    self.info_url, headers=headers, timeout=5.0
                )
                if resp.status_code != 200:
                    raise HTTPException(
                        status_code=503,
                        detail=self.EXCEPTION_ERROR,
                    )
                data = resp.json()
        except ConnectError:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        except asyncio.TimeoutError:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        except ResponseNotRead:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        except InvalidURL:
            raise HTTPException(status_code=503, detail=self.EXCEPTION_ERROR)
        else:
            return data

    async def get_user_info(self, token: str) -> dict | None:
        cached = await self.get_from_cache(token=token)
        if not cached:
            cached = await self.get_from_keycloak(token=token)
            await self.set_in_cache(token=token, value=cached)
        return cached
