import datetime
import time
from logging import getLogger
from threading import Lock
from typing import Any

from .accessor import KeycloakAccessor


class TokenManager:
    def __init__(self, keycloak_service: KeycloakAccessor):
        self.keycloak_service = keycloak_service
        self.token: str | None = None
        self.expires_at: float = 0
        self.lock = Lock()
        self.logger = getLogger("TokenManager")

    def get_token(self) -> str:
        self.logger.debug("Get token invoked")
        with self.lock:
            current_time = time.time()
            if not self.token or current_time >= self.expires_at:
                self._refresh_token()
            self.logger.debug(
                f"KEYCLOAK TOKEN FOR KAFKA: ...{self.token[-3:]}"
                f" EXPIRED_TIME:{datetime.datetime.fromtimestamp(int(self.expires_at))}"
            )
            return self.token

    def _refresh_token(self) -> None:
        token, expires_at = self.keycloak_service.get_token()
        self.token = token
        self.expires_at = expires_at

    def get_token_callback(self, conf: Any) -> tuple[str, float]:
        try:
            token = self.get_token()
            return token, self.expires_at
        except Exception as ex:
            self.logger.exception("Failed to fetch token: %s", ex)
            return "", 0.0
