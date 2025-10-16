import time
from logging import getLogger
from typing import TYPE_CHECKING

from keycloak import KeycloakOpenID

if TYPE_CHECKING:
    from core.app import Application


class KeycloakAccessor(object):
    def __init__(self, app: "Application"):
        self.client = KeycloakOpenID(
            server_url=app.config.keycloak.url,
            client_id=app.config.keycloak.client_id,
            realm_name=app.config.keycloak.realm,
            client_secret_key=app.config.keycloak.client_secret,
        )
        self.logger = getLogger("KeycloakService")

    def get_token(self) -> tuple[str, float]:
        attempt = 5
        while attempt > 0:
            try:
                tkn = self.client.token(grant_type="client_credentials")
                token = tkn["access_token"]
                expires_in = float(tkn["expires_in"]) * 0.95
                return token, time.time() + expires_in
            except Exception as ex:
                print(f"Error fetching token: {ex}")
                time.sleep(1)
                attempt -= 1
        raise RuntimeError(
            "Failed to fetch token from Keycloak after multiple attempts"
        )
