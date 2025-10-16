# import asyncio
# import datetime
# import time
#
# from logging import getLogger
# from typing import TYPE_CHECKING
# from urllib.parse import urlunparse
#
# from aiokafka.abc import AbstractTokenProvider
# from httpx import AsyncClient, BasicAuth, ConnectTimeout
# from keycloak import KeycloakOpenID
#
#
# if TYPE_CHECKING:
#     from core.app import Application
#
#
# class AsyncCustomTokenProvider(AbstractTokenProvider):
#     def __init__(self, app: "Application", **conf):
#         super().__init__(**conf)
#         self.logger = getLogger("KAFKA_Token_Provider")
#         self.tkn = ""
#         self.expired = time.time()
#         self.timeout = 300
#         self.config = app.config
#         url = urlunparse(
#             (
#                 str(app.config.keycloak.protocol),
#                 f"{app.config.keycloak.host}:{app.config.keycloak.port}",
#                 "auth",
#                 "",
#                 "",
#                 "",
#             )
#         )
#         self.keycloak_openid = KeycloakOpenID(
#             server_url=url,
#             client_id=self.config.keycloak.client_id,
#             realm_name=self.config.keycloak.realm,
#             client_secret_key=self.config.keycloak.client_secret,
#         )
#
#     async def retrieve_token_old(self, first_time: bool = False) -> None:
#         dt_pattern = "%Y-%m-%d %H:%M:%S"
#         self.logger.info(msg="Starting...")
#         try:
#             while True:
#                 if self.expired < time.time() + self.timeout or first_time:
#                     payload = {
#                         "grant_type": "client_credentials",
#                         "scope": str(self.config.keycloak.scope),
#                     }
#                     b_auth = BasicAuth(
#                         username=self.config.keycloak.client_id,
#                         password=self.config.keycloak.client_secret,
#                     )
#                     attempt = 5
#                     while attempt > 0:
#                         try:
#                             async with AsyncClient() as session:
#                                 resp = await session.post(
#                                     self.config.keycloak.url,
#                                     auth=b_auth,
#                                     data=payload,
#                                 )
#                                 self.logger.info(msg="Got token for Kafka.")
#                                 if resp.status_code == 200:
#                                     tkn = resp.json()
#                                     break
#                                 else:
#                                     await asyncio.sleep(1)
#                                     attempt -= 1
#                                     continue
#                         except ConnectTimeout:
#                             await asyncio.sleep(1)
#                             self.logger.error(msg="Connection Error.")
#                             attempt -= 1
#                         except asyncio.CancelledError:
#                             self.logger.error(msg="Stopped.")
#                             return
#                         except Exception as e:
#                             self.logger.error(msg=f"{type(e)}: {e}.")
#                             attempt -= 1
#                     else:
#                         self.logger.error(
#                             msg="Token verification service unavailable."
#                         )
#                         if first_time:
#                             return
#                         else:
#                             continue
#                     self.tkn = tkn["access_token"]
#                     self.expired = time.time() + tkn["expires_in"]
#                     self.timeout = tkn["expires_in"] * 0.9
#                     expired_time = datetime.datetime.strftime(
#                         datetime.datetime.fromtimestamp(self.expired),
#                         dt_pattern,
#                     )
#                     self.logger.debug(
#                         msg=f"KEYCLOAK TOKEN FOR KAFKA: ...{self.tkn[-3:]} EXPIRED_TIME:{expired_time}."
#                     )
#                     if first_time:
#                         break
#                 else:
#                     self.logger.info(
#                         msg=f"Waiting for {self.timeout} sec for get new token."
#                     )
#                     await asyncio.sleep(self.timeout)
#         except asyncio.CancelledError:
#             self.logger.info(msg="Stopped.")
#
#     async def retrieve_token(self, first_time: bool = False) -> None:
#         try:
#             while True:
#                 if self.expired < time.time() + self.timeout or first_time:
#                     attempt = 5
#                     while attempt > 0:
#                         try:
#                             tkn = self.keycloak_openid.token(
#                                 grant_type="client_credentials"
#                             )
#                             self.logger.info("Got token for Kafka.")
#                             break
#                         except Exception as ex:
#                             self.logger.error(
#                                 f"Error while fetching token: {ex}"
#                             )
#                             await asyncio.sleep(1)
#                             attempt -= 1
#                     else:
#                         self.logger.error(
#                             "Token verification service unavailable."
#                         )
#                         if first_time:
#                             return
#                         else:
#                             continue
#
#                     self.tkn = tkn["access_token"]
#                     self.expired = time.time() + tkn["expires_in"]
#                     self.timeout = tkn["expires_in"] - 20
#
#                     expired_time = time.strftime(
#                         "%Y-%m-%d %H:%M:%S", time.localtime(self.expired)
#                     )
#                     self.logger.info(
#                         f"KEYCLOAK TOKEN FOR KAFKA: ...{self.tkn[-3:]} EXPIRED_TIME:{expired_time}."
#                     )
#
#                     if first_time:
#                         break
#                 else:
#                     self.logger.info(
#                         f"Waiting for {self.timeout} sec to get a new token."
#                     )
#                     await asyncio.sleep(self.timeout)
#         except asyncio.CancelledError:
#             self.logger.info("Token fetching stopped.")
#
#     async def token(self):
#         return self.tkn
#
#     def _token(self):
#         return self.tkn
