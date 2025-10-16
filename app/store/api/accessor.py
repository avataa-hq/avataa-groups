import asyncio
from logging import getLogger
from typing import TYPE_CHECKING

import authlib.integrations.base_client.errors
import httpx
from authlib.integrations.httpx_client import AsyncOAuth2Client
from base.base_accessor import BaseAccessor
from pydantic import BaseModel
from schemas.schema_group import GroupSchema

if TYPE_CHECKING:
    from core.app import Application


class APIAccessor(BaseAccessor):
    def __init__(self, app: "Application", *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        self.logger = getLogger("API_accessor")
        self.logger.info(msg="created")
        self.client: AsyncOAuth2Client | None = None
        self.keycloak_url = app.config.keycloak.url

    async def connect(self, app: "Application"):
        self.client = AsyncOAuth2Client(self.app.config.api.API_CLIENT)
        result = False
        if self.app.config.kafka.secured:
            while not result:
                try:
                    result = await self.client.fetch_token(
                        self.keycloak_url,
                        username=self.app.config.api.API_USERNAME,
                        password=self.app.config.api.API_PASSWORD,
                    )
                    self.logger.info("Got token for API.")
                except asyncio.CancelledError:
                    self.logger.warning("Api Accessor stopped.")
                    await self.client.aclose()
                except (
                    httpx.TimeoutException,
                    httpx.ConnectError,
                    httpx.ConnectTimeout,
                ) as ex:
                    self.logger.error(
                        msg=f"Error on fetch token for API: {type(ex)} - {ex}"
                    )
                    await asyncio.sleep(60)

    async def disconnect(self, app: "Application"):
        if self.client:
            await self.client.aclose()
        self.logger.info("Api Accessor stopped.")

    async def search_get_data(
        self, group_schema: GroupSchema, mo_ids: list = None
    ):
        if group_schema.group_type_id == 2:
            if group_schema.column_filters:
                correct_filter = APIAccessor._update_query_filter(
                    group_schema.column_filters
                )
            elif mo_ids:
                correct_filter = [
                    {
                        "columnName": "id",
                        "rule": "and",
                        "filters": [
                            {
                                "operator": "isAnyOf",
                                "value": [str(mo_id) for mo_id in mo_ids],
                            }
                        ],
                    }
                ]
            else:
                raise ValueError("Not enough data for the request")
        data_from_search = list()
        statistic = list()
        valid_statistic = set()
        try:
            if not self.client.token or self.client.token.is_expired():
                await self.client.fetch_token(
                    self.keycloak_url,
                    username=self.app.config.api.API_USERNAME,
                    password=self.app.config.api.API_PASSWORD,
                )
            data_from_search: list = await self._collect_data_from_search(
                group=group_schema, correct_filter=correct_filter
            )
        except asyncio.CancelledError:
            self.logger.info(msg=" Api Accessor Stop.")
        except httpx.TimeoutException:
            self.logger.error(msg="OAuth2 error: Timeout Exception.")
        except authlib.integrations.base_client.errors.InvalidTokenError:
            self.logger.error(msg="except Invalid Token.")
            await self.client.fetch_token(
                self.keycloak_url,
                username=self.app.config.api.API_USERNAME,
                password=self.app.config.api.API_PASSWORD,
            )
        except ValueError as ex:
            self.logger.error(msg=f"Empty data from inventory: {ex}")
            raise ex
        except RuntimeError as ex:
            self.logger.error(msg=f"Error: {type(ex)}: {ex}.")
            raise ex

        if len(data_from_search) > (group_schema.min_qnt or 0):
            statistic, valid_statistic = self._create_statistic_from_data(
                data=data_from_search, group=group_schema
            )
        return statistic, valid_statistic

    @staticmethod
    def _update_query_filter(query: list) -> list:
        """For default correct request to MS Search we must add field "status" with value "isNotEmpty".
        In case user add this field, we leave it as is."""
        for el in query:  # type: dict
            if el.get("columnName", None) == "status":
                break
        else:
            query.append(
                {
                    "filters": [{"value": "", "operator": "isNotEmpty"}],
                    "rule": "and",
                    "columnName": "status",
                }
            )
        return query

    async def _collect_data_from_search(
        self, group: GroupSchema, correct_filter: list
    ) -> list:
        error_message = "Error: %s. Address to connect: %s"
        output = []
        step: int = 100_000  # optimal limit for Elastic
        offset: int = 0
        max_retries = 3
        timeout = 1
        read_timeout = 8
        resp = None
        if group.group_type_id == 1:
            address = (
                f"{self.app.config.api.SEARCH_CLIENT_PROTOCOL}://{self.app.config.api.SEARCH_CLIENT_HOST}:"
                f"{self.app.config.api.SEARCH_CLIENT_PORT}"
                f"{self.app.config.api.SEARCH_CLIENT_PREFIX}/inventory/get_inventory_objects_by_filters"
            )
        else:
            address = (
                f"{self.app.config.api.SEARCH_CLIENT_PROTOCOL}://{self.app.config.api.SEARCH_CLIENT_HOST}:"
                f"{self.app.config.api.SEARCH_CLIENT_PORT}"
                f"{self.app.config.api.SEARCH_CLIENT_PREFIX}/severity/processes"
            )
        total_count: int = offset + 1
        data_input = {
            "tmoId": group.tmo_id,
            "withGroups": False,
            "limit": {"limit": step, "offset": offset},
            "columnFilters": correct_filter,
        }
        try:
            while total_count >= offset and max_retries:
                resp = await self.client.post(
                    address, json=data_input, timeout=read_timeout
                )
                self.logger.debug(msg=f"{resp}")
                if resp.status_code == 200:
                    total_count = resp.json().get("totalCount", None)
                    output.extend(resp.json().get("rows", None))
                    offset += step
                    data_input["limit"]["offset"] = offset
                elif resp.status_code == 400:
                    msg = f"MS Search error: URL: {address} Status: {resp.status_code} Detail: {resp.json()}"
                    raise ValueError(msg)
                elif resp.status_code == 403:
                    await self.client.fetch_token(
                        self.keycloak_url,
                        username=self.app.config.api.API_USERNAME,
                        password=self.app.config.api.API_PASSWORD,
                    )
                else:
                    msg = f"MS Search error: URL: {address} Status: {resp.status_code} Detail: {resp.json()}"
                    self.logger.error(msg)
                    await asyncio.sleep(timeout)
                    max_retries -= 1
            if max_retries == 0 and resp:
                raise RuntimeError(
                    f"MS Search error: URL: {address} Status: {resp.status_code} Detail: {resp.json()}"
                )
        except ConnectionError as ex:
            self.logger.exception(error_message, ex, address)
            raise RuntimeError("Cant Connect to MS Search")
        except httpx.ReadTimeout as ex:
            self.logger.exception(error_message, ex, address)
        except ValueError as ex:
            self.logger.exception(error_message, ex, address)
            raise ex
        except Exception as ex:
            self.logger.exception(ex)
            raise ex
        return output

    def _create_statistic_from_data(
        self, data: list, group: GroupSchema
    ) -> (list, set):
        statistic: list[BaseModel] = list()
        valid_statistic = set()
        for el in data:
            set_fields_from_data = set([k for k, _ in el.items()])
            data_for_new_statistic = {
                "Camunda": el,
                "TPRM": el,
                "TMO": {"tmo_id": el["tmo_id"]},
                "MO": el,
                "groupName": group.group_name,
            }
            set_fields_from_model = set()
            exclude_fields = {
                "geometry",
                "groupName",
                "latitude",
                "longitude",
                "model",
                "p_id",
                "point_a_id",
                "point_b_id",
                "pov",
                "status",
                "version",
            }
            for k, v in self.app.store.group_scheme[
                f"{group.tmo_id}"
            ].model_fields.items():
                if v.annotation is str:
                    set_fields_from_model.add(k)
                elif k in ["MO", "Camunda", "TPRM"]:
                    for inner_k, inner_v in v.annotation.model_fields.items():
                        set_fields_from_model.add(inner_k)

            set_fields_from_model -= exclude_fields
            valid_statistic = set_fields_from_model - set_fields_from_data

            statistic.append(
                self.app.store.group_scheme[f"{group.tmo_id}"](
                    **data_for_new_statistic
                )
            )
        return statistic, valid_statistic
