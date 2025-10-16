import asyncio
import json
import pickle
from datetime import date, datetime
from logging import getLogger
from typing import TYPE_CHECKING, Union

import grpc
from base.base_accessor import BaseAccessor
from crud.crud_group import crud_group
from google.protobuf import json_format
from grpc.aio import AioRpcError
from models.model_group import GroupModel
from pydantic import BaseModel, ConfigDict, create_model
from schemas.schema_group import (
    CamundaSchema,
    GroupBase,
    GroupSchema,
    TMOSchema,
)
from schemas.schema_group_template import GroupTemplateMain

from store.grpc.protobuf import (
    from_group_to_search_pb2,
    from_group_to_search_pb2_grpc,
    inventory_data_pb2,
    inventory_data_pb2_grpc,
)
from store.grpc.protobuf.from_group_to_search_pb2 import ResponseProcessesGroups

if TYPE_CHECKING:
    from core.app import Application


mapping_sql_python: dict = {
    "BOOLEAN": bool,
    "INTEGER": int,
    "FLOAT": float,
    "JSON": dict,
    "VARCHAR": str,
    "DATETIME": datetime,
    "int": int,
    "str": str,
    "float": float,
    "bool": bool,
    "date": date,
    "datetime": datetime,
    "mo_link": str,
    "prm_lin": str,
    "formula": str,
    "user_link": str,
    "enum": str,
}


class GRPCAccessor(BaseAccessor):
    def __init__(self, app: "Application", *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        self.logger = getLogger("gRPC_Accessor")
        self.channel_inventory: grpc.Channel | None = None
        self.channel_search: grpc.Channel | None = None

        self.start_timeout = 60

    async def connect(self, app: "Application") -> None:
        channel_options = [
            ("grpc.keepalive_time_ms", 15_000),
            ("grpc.keepalive_timeout_ms", 32_000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ]
        self.channel_inventory = grpc.aio.insecure_channel(
            target=f"{app.config.api.INVENTORY_HOST}:{app.config.grpc.INVENTORY_GRPC_PORT}",
            options=channel_options,
        )
        self.channel_search = grpc.aio.insecure_channel(
            f"{app.config.api.SEARCH_CLIENT_HOST}:{app.config.grpc.SEARCH_GRPC_PORT}"
        )
        model_created = False
        self.logger.info(msg="Creating dynamic model...")
        while not model_created:
            try:
                model_created = await self.generate_dynamic_pydantic_model()
            except ValueError as ex:
                self.logger.error(
                    "Can't create dynamic pydantic model. Reason %s", ex
                )
                raise ex
            except OSError as ex:
                self.logger.error(msg=f"Couldn't connect to {ex}.")
                await asyncio.sleep(60)
            except Exception as ex:
                self.logger.error(msg=f"{type(ex)}: {ex}")

    async def disconnect(self, app: "Application"):
        if self.channel_inventory:
            await self.channel_inventory.close()
        if self.channel_search:
            await self.channel_search.close()
        self.logger.info(msg="consumer stopped.")

    # async def zeebe_create_process_instance(self, obj_in: BaseModel, tmo_id: int) -> int:
    #     stub = gateway_pb2_grpc.GatewayStub(self.channel_zeebe_server)
    #     if not obj_in.Camunda.processDefinitionKey or not obj_in.Camunda.processDefinitionVersion:
    #         pdk, version = (await self.inventory_get_tmo_info([tmo_id]))[tmo_id]
    #         aggregated_statistic = {}
    #         aggregated_statistic |= obj_in.MO.model_dump(by_alias=True, exclude_none=True)
    #         aggregated_statistic |= obj_in.TPRM.model_dump(by_alias=True)
    #         aggregated_statistic |= obj_in.Camunda.model_dump(by_alias=True)
    #         aggregated_statistic |= {"groupName": obj_in.groupName}
    #         msg = gateway_pb2.CreateProcessInstanceRequest(
    #             bpmnProcessId=pdk,
    #             version=version,
    #             variables=json.dumps(aggregated_statistic, default=serialize_datetime),
    #         )
    #     else:
    #         aggregated_statistic = {}
    #         aggregated_statistic |= obj_in.MO.model_dump(by_alias=True, exclude_none=True)
    #         aggregated_statistic |= obj_in.TPRM.model_dump(by_alias=True)
    #         aggregated_statistic |= obj_in.Camunda.model_dump(by_alias=True)
    #         aggregated_statistic |= {"groupName": obj_in.groupName}
    #         msg = gateway_pb2.CreateProcessInstanceRequest(
    #             bpmnProcessId=obj_in.Camunda.processDefinitionKey,
    #             version=obj_in.Camunda.processDefinitionVersion,
    #             variables=json.dumps(aggregated_statistic, default=serialize_datetime),
    #         )
    #     try:
    #         response = await stub.CreateProcessInstance(msg)
    #         message_as_dict = json_format.MessageToDict(
    #             response, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
    #         )
    #         return int(message_as_dict["processInstanceKey"])
    #     except Exception as ex:
    #         self.logger.error(f"{type(ex)}: {ex}.")

    async def inventory_get_tmo_info(
        self, tmo_id: list[int]
    ) -> dict[int, tuple[str, int]]:
        stub = inventory_data_pb2_grpc.InformerStub(self.channel_inventory)
        msg = inventory_data_pb2.TMOInfoRequest(tmo_id=tmo_id)
        try:
            response = await stub.GetTMOInfoByTMOId(msg)
        except Exception as ex:
            self.logger.error(f"{type(ex)}: {ex}")
            raise ConnectionError(
                f"GRPC {self.channel_inventory} is unavailable"
            )
        result = pickle.loads(bytes.fromhex(response.tmo_info))
        # tmo_id: (process_definition_id, version)
        output: dict = {}
        for k in result.keys():
            name, version = result[k]["lifecycle_process_definition"].split(":")
            output[k] = (name, int(version))
        return output

    # async def update_process_instance_variables(self, obj_in: BaseModel, process_instance_key: int) -> None:
    #     stub = gateway_pb2_grpc.GatewayStub(self.channel_zeebe_server)
    #     aggregated_statistic = {}
    #     aggregated_statistic |= obj_in.MO.model_dump(by_alias=True, exclude_none=True)
    #     aggregated_statistic |= obj_in.TPRM.model_dump(by_alias=True)
    #     aggregated_statistic |= obj_in.Camunda.model_dump(by_alias=True)
    #     aggregated_statistic |= {"groupName": obj_in.groupName}
    #     msg = gateway_pb2.SetVariablesRequest(
    #         elementInstanceKey=process_instance_key,
    #         variables=json.dumps(aggregated_statistic, default=serialize_datetime),
    #         local=False,
    #     )
    #     try:
    #         await stub.SetVariables(msg)
    #     except Exception as ex:
    #         self.logger.error(f"{type(ex)}: {ex}")
    #
    # async def cancel_process_instance(
    #     self,
    #     process_instance_key: list[int],
    # ) -> None:
    #     stub = gateway_pb2_grpc.GatewayStub(self.channel_zeebe_server)
    #     for key in process_instance_key:
    #         msg = gateway_pb2.CancelProcessInstanceRequest(processInstanceKey=key)
    #         try:
    #             await stub.CancelProcessInstance(msg)
    #         except Exception as ex:
    #             self.logger.error(f"{type(ex)}: {ex}")

    async def inventory_get_info(
        self,
        current_group: GroupSchema,
        mo_ids: list[int],
    ):
        stub = inventory_data_pb2_grpc.InformerStub(self.channel_inventory)
        msg = inventory_data_pb2.RequestForFilteredObjInfoByTMO(
            object_type_id=current_group.tmo_id, mo_ids=mo_ids
        )
        try:
            result = stub.GetFilteredObjWithParamsStream(msg)
            response = []
            async for res in result:
                response.append(res)
        except Exception as ex:
            self.logger.exception(ex)
            raise ConnectionError(
                f"GRPC {self.channel_inventory} is unavailable"
            )
        message_as_dict = json_format.MessageToDict(
            response,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        statistic: list[BaseModel] = []
        valid_statistic: bool = True
        for mo_info in message_as_dict["objects_with_parameters"]:
            result = pickle.loads(bytes.fromhex(mo_info))
            data_for_new_statistic = {
                "Camunda": {},
                "TPRM": {},
                "TMO": {"tmo_id": current_group.tmo_id},
                "MO": {},
                "groupName": current_group.group_name,
            }
            for k, v in result.items():
                if k != "params":
                    data_for_new_statistic["MO"].update({k: v})
                else:
                    for tprm in result[k]:  # type: dict
                        data_for_new_statistic["TPRM"].update(
                            {str(tprm["id"]): tprm["value"]}
                        )
            statistic.append(
                self.app.store.group_scheme[str(current_group.tmo_id)](
                    **data_for_new_statistic
                )
            )
        return statistic, valid_statistic

    async def search_get_statistic_from_mo(
        self, group_schema: GroupSchema, mo_ids: list[int] | None = None
    ):
        if group_schema.group_type_id != 1:
            raise ValueError("Incorrect group type!")
        data_from_search = list()
        statistic = list()
        valid_statistic = set()

        if group_schema.column_filters:
            correct_filter = GRPCAccessor._update_query_filter(
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
        try:
            data_from_search: list = await self._collect_data_from_search(
                group=group_schema, correct_filter=correct_filter
            )
        except Exception as ex:
            self.logger.exception(ex)
        if len(data_from_search) > (group_schema.min_qnt or 0):
            statistic, valid_statistic = self._create_statistic_from_data(
                data=data_from_search, group=group_schema
            )
        return statistic, valid_statistic

    # async def zeebe_get_existed_entities(
    #     self, group: GroupModel, mo_ids: list[int] = None
    # ) -> (list[BaseModel], set, list[BaseModel]):
    #     if not group.tmo_id:
    #         raise ValueError("Empty object type")
    #     if not mo_ids and not group.column_filters:
    #         raise ValueError("Empty group")
    #
    #     limit: int = 1000
    #     offset: int = 0
    #     total_count: int = offset + 1
    #     data_from_zeebe: list[str] = []
    #     stub = zeebe_severity_pb2_grpc.SeverityStub(self.channel_zeebe_client_server)
    #     self.logger.info(msg=f"Start response for: {group.group_name}")
    #     start_time = time.perf_counter()
    #     while total_count >= offset:
    #         lmt = json.dumps({"limit": limit, "offset": offset})
    #         if group.column_filters:
    #             prepared_filter_list = [json.dumps(fltr) for fltr in group.column_filters]
    #             msg = zeebe_severity_pb2.ProcessesInput(
    #                 tmo_ids=[group.tmo_id], limit=lmt, filters_list=prepared_filter_list
    #             )
    #         else:
    #             msg = zeebe_severity_pb2.ProcessesInput(tmo_ids=[group.tmo_id], limit=lmt, mo_ids=mo_ids)
    #
    #         try:
    #             response = await stub.GetProcesses(msg)
    #         except grpc.aio.AioRpcError as ex:
    #             self.logger.error(f"{type(ex)}: {ex}")
    #             raise ValueError(f"GRPC error {ex}")
    #         except Exception as ex:
    #             self.logger.error(f"{type(ex)}: {ex}")
    #             raise ValueError("GRPC unavailable.")
    #         message_as_dict = json_format.MessageToDict(
    #             response, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
    #         )
    #         data_from_zeebe.extend(message_as_dict["rows"])
    #         total_count = message_as_dict["total_count"]
    #         offset += limit
    #
    #     end_time = time.perf_counter()
    #     self.logger.info(msg=f"Get data for {group.group_name} | {end_time - start_time} {start_time=} {end_time=}")
    #     if not data_from_zeebe:
    #         raise ValueError("Wrong data from Zeebe.")
    #     statistic: list[BaseModel] = []
    #     valid_statistic: set = set()
    #     for el in data_from_zeebe:
    #         el = json.loads(el)
    #         set_fields_from_data = set([k for k, _ in el.items()])
    #
    #         data_for_new_statistic = {
    #             "Camunda": el,
    #             "TPRM": el,
    #             "TMO": {"tmo_id": el["tmo_id"]},
    #             # "MO": {"id": el.get("id"), "name": el.get("name"),"tmo_id": el.get("tmo_id"), "active"},
    #             "MO": el,
    #             "groupName": group.group_name,
    #         }
    #         set_fields_from_model = set()
    #         exclude_fields = {
    #             "geometry",
    #             "groupName",
    #             "latitude",
    #             "longitude",
    #             "model",
    #             "p_id",
    #             "point_a_id",
    #             "point_b_id",
    #             "pov",
    #             "status",
    #             "version",
    #         }
    #         for k, v in self.app.store.group_scheme[f"{group.tmo_id}"].model_fields.items():
    #             if v.annotation == str:
    #                 set_fields_from_model.add(k)
    #             elif k in ["MO", "Camunda", "TPRM"]:
    #                 for inner_k, inner_v in v.annotation.model_fields.items():
    #                     set_fields_from_model.add(inner_k)
    #
    #         set_fields_from_model -= exclude_fields
    #         valid_statistic = set_fields_from_model - set_fields_from_data
    #         # # TODO add list invalid fields to frontend
    #         # if missing_fields != {"groupName"}:
    #         #     valid_statistic = False
    #         # statistic.append(self.app.store.group_scheme[f"{group.tmo_id}"](**el))
    #
    #         statistic.append(self.app.store.group_scheme[f"{group.tmo_id}"](**data_for_new_statistic))
    #
    #     return statistic, valid_statistic

    async def create_dynamic_statistic_model(self, tmo_id: int):
        stub = inventory_data_pb2_grpc.InformerStub(self.channel_inventory)
        msg = inventory_data_pb2.RequestTMOAttrsAndTypes(tmo_id=tmo_id)
        try:
            response = await stub.GetColumnsForMaterializedView(
                msg, wait_for_ready=True
            )
        except TypeError as ex:
            self.logger.exception(ex)
            raise TypeError(ex)
        except AioRpcError as ex:
            self.logger.exception(ex)
            raise ConnectionError(
                f"GRPC {self.channel_inventory} is unavailable"
            )
        except Exception as ex:
            self.logger.exception(ex)
            raise ConnectionError(
                f"GRPC {self.channel_inventory} is unavailable"
            )
        message_as_dict = json_format.MessageToDict(
            response,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        if not message_as_dict["attrs"]:
            raise ValueError("Wrong tmo_id")
        try:
            self._create_model(
                tmo_id=tmo_id, mo_and_tprm_data=message_as_dict["attrs"]
            )
        except KeyError as ex:
            self.logger.error(
                msg=f"Incorrect model for {tmo_id=}. TPRM type {ex} is not existed."
            )
            raise KeyError(ex)
        except Exception as ex:
            self.logger.exception(ex)

    def update_dynamic_statistic_model(self, tmo_id: int, group_type_id: int):
        pass

    def _create_model(self, tmo_id: int, mo_and_tprm_data: list[dict]):
        tmo_fields = {}
        mo_fields = {}
        tprm_fields = {}
        camunda_fields = {}
        for attrs in mo_and_tprm_data:
            if attrs["name"].isdigit():
                # Skip multiple mo_link
                if attrs.get("multiply") and attrs.get("type") == "mo_link":
                    self.logger.info(
                        "Statistic won't be saved for multiple mo_link with tprm id: %s.",
                        attrs["name"],
                    )
                    continue
                tprm_fields.setdefault(
                    f"{attrs['name']}",
                    (Union[mapping_sql_python[attrs["type"]], None], None),
                )
            else:
                mo_fields.setdefault(
                    f"{attrs['name']}",
                    (Union[mapping_sql_python[attrs["type"]], None], None),
                )

        group_statistic_fields = {}
        tmo_model = create_model(
            "TMO",
            **tmo_fields,
            __base__=TMOSchema,
        )
        mo_model = create_model(
            "MO",
            **mo_fields,
            __config__=ConfigDict(json_encoders={datetime: convert_datetime}),
        )
        tprm_model = create_model(
            str(tmo_id),
            **tprm_fields,
            __config__=ConfigDict(json_encoders={datetime: convert_datetime}),
        )
        camunda_model = create_model(
            "Camunda",
            **camunda_fields,
            __base__=CamundaSchema,
        )
        group_statistic_fields.setdefault("TMO", (tmo_model, None))
        group_statistic_fields.setdefault("MO", (mo_model, None))
        group_statistic_fields.setdefault("TPRM", (tprm_model, None))
        group_statistic_fields.setdefault("Camunda", (camunda_model, None))
        group_statistic_fields.setdefault("groupName", (str, None))
        group_statistic = create_model(f"{tmo_id}", **group_statistic_fields)
        if self.app.store.group_scheme.get(f"{tmo_id}", None):
            self.app.store.group_scheme[f"{tmo_id}"] = group_statistic
        else:
            self.app.store.group_scheme.setdefault(f"{tmo_id}", group_statistic)
        self.logger.info(f"Generated model with tmo_id: {tmo_id}.")

    async def generate_dynamic_pydantic_model(self) -> bool:
        result: bool = False  # This method inside while cycle
        try:
            async with self.app.database.session() as session:
                list_all_groups: list[
                    GroupModel
                ] = await crud_group.get_all_group(
                    session=session, limit=10_000
                )
        except (AttributeError, OSError, RuntimeError, ValueError) as ex:
            self.logger.error(
                "Error occurred in generate_dynamic_pydantic_model: %s.",
                ex.args[0],
            )
            return result
            # self.logger.exception(ex)
        except Exception as ex:
            self.logger.error(msg=f"{type(ex)}: {ex}")
            await asyncio.sleep(1)
            return result
        if list_all_groups:
            total_tmo_id = len({gr.tmo_id for gr in list_all_groups})
            while len(self.app.store.group_scheme) != total_tmo_id:
                try:
                    for group in list_all_groups:
                        if not self.app.store.group_scheme.get(
                            str(group.tmo_id), None
                        ):
                            try:
                                await self.create_dynamic_statistic_model(
                                    tmo_id=group.tmo_id
                                )
                            except ValueError as ex:
                                self.logger.error(
                                    "%s: %s for group: %s",
                                    ex,
                                    group.tmo_id,
                                    group.group_name,
                                )
                                async with (
                                    self.app.database.session() as session
                                ):
                                    await crud_group.remove_groups(
                                        session=session, obj_in=[group]
                                    )
                                self.logger.error(
                                    "Group: %s was removed.", group.group_name
                                )
                                continue
                            except KeyError:
                                break
                            except Exception as ex:
                                self.logger.error(
                                    msg=f"Error on start connection: {type(ex)}: {ex}."
                                )
                                raise ConnectionError(
                                    "gRPC server not available."
                                )
                        else:
                            continue
                except Exception as ex:
                    self.logger.error(f"Can't connect to DB: {ex}")
                    await asyncio.sleep(self.start_timeout)
        result = True
        return result

    async def get_processes_group_from_search(
        self, group_template: GroupTemplateMain
    ) -> list[ResponseProcessesGroups]:
        stub = from_group_to_search_pb2_grpc.GroupSearchStub(
            self.channel_search
        )
        column_filters = json.dumps(group_template.column_filters)
        msg = from_group_to_search_pb2.RequestGetProcessesGroups(
            tmo_id=group_template.tmo_id,
            filters_list=column_filters,
            ranges_object=json.dumps(group_template.ranges_object),
            with_groups=False,
            group_by=json.dumps(group_template.identical),
            min_group_qty=group_template.min_qnt,
        )
        result = []

        try:
            async for response in stub.GetProcessesGroups(msg):
                result.append(response)
        except grpc.aio.AioRpcError as ex:
            self.logger.exception(ex)
            raise ValueError(ex.details())
        self.logger.info("\nRequest %s\nGet Data size: %d", msg, len(result))
        return result

    async def get_severity_processes(
        self, group_schema: GroupBase, mo_ids: list = None
    ):
        statistic = list()
        valid_statistic = set()
        if group_schema.column_filters:
            group_schema.column_filters = GRPCAccessor._update_query_filter(
                group_schema.column_filters
            )
        elif mo_ids:
            group_schema.column_filters = [
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
        stub = from_group_to_search_pb2_grpc.GroupSearchStub(
            self.channel_search
        )
        limit = 10_000
        offset = 0
        result = []
        while offset <= len(result) + 1:
            msg = from_group_to_search_pb2.RequestGetProcesses(
                tmo_id=group_schema.tmo_id,
                filters_list=json.dumps(group_schema.column_filters)
                if group_schema.column_filters
                else None,
                ranges_object=json.dumps(group_schema.ranges_object)
                if group_schema.ranges_object
                else None,
                with_groups=False,
                limit=json.dumps({"limit": limit, "offset": offset}),
            )
            offset += limit
            try:
                async for response in stub.GetProcesses(msg):
                    entity = json.loads(response.mo)
                    result.append(entity)
            except grpc.aio.AioRpcError as ex:
                self.logger.exception(ex)
                self.logger.warning("Current message to gRPC: %s", msg)
                raise ValueError(ex.details())
        if len(result) > (group_schema.min_qnt or 0):
            statistic, valid_statistic = self._create_statistic_from_data(
                data=result, group=group_schema
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

    def _create_statistic_from_data(
        self, data: list, group: GroupBase
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
            try:
                for k, v in self.app.store.group_scheme[
                    f"{group.tmo_id}"
                ].model_fields.items():
                    if v.annotation is str:
                        set_fields_from_model.add(k)
                    elif k in ["MO", "Camunda", "TPRM"]:
                        for (
                            inner_k,
                            inner_v,
                        ) in v.annotation.model_fields.items():
                            set_fields_from_model.add(inner_k)
            except KeyError as ex:
                self.logger.warning(
                    "Group Scheme: %s", self.app.store.group_scheme
                )
                self.logger.exception(
                    "Can't find model for tmo with id: %s. Check auto model generation",
                    ex.args[0],
                )

            set_fields_from_model -= exclude_fields
            valid_statistic = set_fields_from_model - set_fields_from_data

            statistic.append(
                self.app.store.group_scheme[f"{group.tmo_id}"](
                    **data_for_new_statistic
                )
            )
        return statistic, valid_statistic


def convert_datetime(dt: datetime) -> str:
    pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
    return dt.strftime(pattern)


def serialize_datetime(obj):
    if isinstance(obj, datetime):
        pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
        return obj.strftime(pattern)
