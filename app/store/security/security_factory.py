from core.config import setup_config

from store.security.implementation.disabled import DisabledSecurity
from store.security.implementation.keycloak import Keycloak, KeycloakInfo
from store.security.implementation.utils.user_info_cache import (
    UserInfoCache,
)
from store.security.security_interface import SecurityInterface


class SecurityFactory:
    def get(self, security_type: str) -> SecurityInterface:
        match security_type.upper():
            case "KEYCLOAK":
                return self._get_keycloak()
            case "KEYCLOAK-INFO":
                return self._get_keycloak_info()
            case _:
                return self._get_disabled()

    @staticmethod
    def _get_disabled() -> SecurityInterface:
        return DisabledSecurity()

    def _get_keycloak(self) -> SecurityInterface:
        keycloak_public_url = security_config.keycloak_public_key_url
        token_url = security_config.keycloak_token_url
        authorization_url = security_config.keycloak_authorization_url
        refresh_url = authorization_url
        scopes = {
            "profile": "Read claims that represent basic profile information"
        }

        return Keycloak(
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_keycloak_info(self) -> SecurityInterface:
        keycloak_public_url = security_config.keycloak_public_key_url
        token_url = security_config.keycloak_token_url
        authorization_url = security_config.keycloak_authorization_url
        refresh_url = authorization_url
        scopes = {
            "profile": "Read claims that represent basic profile information"
        }
        cache = UserInfoCache()
        cache_user_info_url = security_config.security_middleware_url
        return KeycloakInfo(
            cache=cache,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
            cache_user_info_url=cache_user_info_url,
        )


security_config = setup_config().security_config
security = SecurityFactory().get(security_config.security_type)
