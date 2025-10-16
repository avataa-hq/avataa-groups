from datetime import date, datetime
from logging import getLogger
from typing import TYPE_CHECKING, Union

import redis.asyncio as redis
from base.base_accessor import BaseAccessor
from models.model_group import GroupModel
from pydantic import BaseModel
from redis.exceptions import DataError
from schemas.schema_group import GroupSchema
from sqlalchemy.exc import MissingGreenlet

if TYPE_CHECKING:
    from core.app import Application


mapper = {
    "Count": "frequency",
    "endDate": "maximum",
    "id": "maximum",
    "processInstanceId": "frequency",
    "processInstanceKey": "frequency",
    "processVersion": "maximum",
    "startDate": "maximum",
    "tmo_id": "frequency",
    "version": "frequency",
    "name": "maximum",
}

T = Union[bool, date, datetime, int, float, str, None]


class RedisAccessor(BaseAccessor):
    def __init__(self, app: "Application", *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        self.logger = getLogger("Redis_Accessor")
        self._pool: redis.ConnectionPool | None = None
        self._redis: redis.Redis | None = None
        self.prefix = "GROUP_MS:"

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            raise RuntimeError(
                "Redis client is not initialized. Call connect() first."
            )
        return self._redis

    async def connect(self, app: "Application"):
        self._pool = redis.ConnectionPool.from_url(
            f"redis://{app.config.redis.host}:{app.config.redis.port}",
            password=app.config.redis.password,
            decode_responses=True,
        )
        self._redis = redis.Redis(
            connection_pool=self._pool,
            protocol=3,
        )

    async def disconnect(self, app: "Application"):
        if self._pool and self.redis:
            await self._pool.disconnect()

    async def _create_hset_for_redis(
        self, data: list[BaseModel], pipe, is_aggregate: bool, group_name: str
    ) -> dict:
        raw_data = {}
        exclude_parameters_name = ["sortValues", "operations", "params"]
        necessary_params = ["active", "tmo_id"]
        for entity in data:  # type: BaseModel
            # TMO, MO, TPRM, Camunda
            for statistic_name, statistic_value in entity.model_dump(
                by_alias=True, exclude={"groupName"}
            ).items():  # type: str, dict
                for parameter_name, parameter_value in statistic_value.items():
                    if (
                        parameter_name not in exclude_parameters_name
                        and parameter_value
                    ):
                        if is_aggregate or parameter_name in necessary_params:
                            path, mapping = self._redis_param_builder(
                                group_name=group_name,
                                statistic_name=statistic_name,
                                parameter_name=parameter_name,
                                parameter_value=parameter_value,
                                entity_id=entity.MO.id,
                            )
                        else:
                            path, mapping = self._redis_param_builder(
                                group_name=group_name,
                                statistic_name=statistic_name,
                                parameter_name=parameter_name,
                                parameter_value=None,
                                entity_id=entity.MO.id,
                            )
                        await pipe.hset(name=path, mapping=mapping)
                        data = (
                            parameter_value
                            if is_aggregate or parameter_name == "tmo_id"
                            else None
                        )
                        raw_data.setdefault(path, []).append(data)
                # raw_data.update(await self._create_path_and_mapping(group_name=group_name,
                #                                                     statistic_name=statistic_name,
                #                                                     statistic_value=statistic_value,
                #                                                     is_aggregate=is_aggregate,
                #                                                     mo_id=entity.MO.id,
                #                                                     pipe=pipe)
                #                 )
                # raw_data.setdefault()
        return raw_data

    async def _create_path_and_mapping(
        self,
        group_name: str,
        statistic_name: str,
        statistic_value: dict,
        is_aggregate: bool,
        mo_id: int,
        pipe,
    ) -> dict:
        exclude_parameters_name = ["sortValues", "operations", "params"]
        necessary_params = ["active", "tmo_id"]
        output = {}
        for parameter_name, parameter_value in statistic_value.items():
            if (
                parameter_name not in exclude_parameters_name
                and parameter_value
            ):
                if is_aggregate or parameter_name in necessary_params:
                    path, mapping = self._redis_param_builder(
                        group_name=group_name,
                        statistic_name=statistic_name,
                        parameter_name=parameter_name,
                        parameter_value=parameter_value,
                        entity_id=mo_id,
                    )
                else:
                    path, mapping = self._redis_param_builder(
                        group_name=group_name,
                        statistic_name=statistic_name,
                        parameter_name=parameter_name,
                        parameter_value=None,
                        entity_id=mo_id,
                    )
                await pipe.hset(name=path, mapping=mapping)
                data = (
                    parameter_value
                    if is_aggregate or parameter_name == "tmo_id"
                    else None
                )
                output.setdefault(path, []).append(data)
        return output

    async def set_statistic_by_schema(
        self, current_group: GroupSchema, data: list[BaseModel]
    ) -> BaseModel:
        pipe = self.redis.pipeline()
        raw_data = await self._create_hset_for_redis(
            data=data,
            pipe=pipe,
            is_aggregate=current_group.is_aggregate,
            group_name=current_group.group_name,
        )
        try:
            await pipe.execute()
            data_for_group_create = {}
            for k, v in raw_data.items():
                result = self._get_aggregated_data(k, v)
                data_for_group_create.setdefault(k.split(":")[2], {}).update(
                    result
                )
            if not data_for_group_create.get("groupName", None):
                data_for_group_create |= {"groupName": current_group.group_name}
            # Create GroupStat Model
            group_stat = self.app.store.group_scheme[f"{current_group.tmo_id}"](
                **data_for_group_create
            )
            return group_stat
        except DataError as ex:
            self.logger.exception("Set statistic error: %s", ex)
            raise ValueError(f"{ex}: {ex.args}")
        except UnboundLocalError as ex:
            self.logger.exception("Set statistic UnboundLocalError: %s", ex)
        except Exception as ex:
            self.logger.warning(msg=f"{type(ex)}: {ex}.")

    async def get_statistic(self, group_model: GroupModel) -> BaseModel:
        self.logger.debug(msg="Start redis function")
        try:
            self.logger.debug(msg="Trying all groups...")
            data_for_group_create = {}
            # Looking for redis hashset with group_name
            all_group_parameters = await self.redis.keys(
                self.prefix + group_model.group_name + ":*"
            )
            self.logger.debug(
                msg=f"Get all group parameters {all_group_parameters}"
            )
            if not all_group_parameters:
                if group_model.group_type_id == 1 and (
                    group_model.column_filters or group_model.elements
                ):
                    data: (
                        list[BaseModel],
                        bool,
                    ) = await self.app.store.grpc.search_get_statistic_from_mo(
                        group_schema=group_model.to_schema(),
                        mo_ids=[el.entity_id for el in group_model.elements],
                    )
                elif group_model.group_type_id == 2:
                    data: (
                        list[BaseModel],
                        bool,
                    ) = await self.app.store.grpc.get_severity_processes(
                        group_schema=group_model.to_schema(),
                        mo_ids=[el.entity_id for el in group_model.elements],
                    )
                else:
                    raise TypeError("Incorrect group Type")
                if not data:
                    raise ValueError("Entities not found in Search")
                return await self.set_statistic_by_schema(
                    current_group=group_model.to_schema(), data=data[0]
                )

            for parameter in all_group_parameters:
                data_from_redis: list = await self.redis.hvals(parameter)
                data_for_group_create.setdefault(
                    parameter.split(":")[2], {}
                ).update(
                    self._get_aggregated_data(
                        prm=parameter, data=data_from_redis
                    )
                )
            if not data_for_group_create.get("groupName", None):
                data_for_group_create |= {"groupName": group_model.group_name}
            # Create GroupStat Model
            # group_stat: BaseModel = self.app.store.group_scheme[f"{group_model.tmo_id}"](**data_for_group_create)
            # return group_stat
        except MissingGreenlet:
            self.logger.warning(
                msg=f"No element in group: {group_model.group_name}."
            )
            data_for_group_create = {}
            # group_stat: BaseModel = self.app.store.group_scheme[f"{group_model.tmo_id}"]()
        except TypeError:
            self.logger.warning("Get statistic Error for data %s", group_model)
            data_for_group_create = {
                "groupName": group_model.group_name,
                "MO": {"tmo_id": group_model.tmo_id},
            }
        except Exception as ex:
            self.logger.exception(ex)
            self.logger.warning(msg=f"Get statistic Error {type(ex)}: {ex}.)")
            self.logger.warning("Current group model: %s", group_model)
            data_for_group_create = {
                "groupName": group_model.group_name,
                "MO": {"tmo_id": group_model.tmo_id},
            }
        finally:
            group_stat: BaseModel = self.app.store.group_scheme[
                f"{group_model.tmo_id}"
            ](**data_for_group_create)
        return group_stat

    async def get_statistic_by_schema(
        self, group_schema: GroupSchema
    ) -> BaseModel:
        # Very slow method
        self.logger.debug(msg="Start redis function")
        try:
            self.logger.debug(msg="Trying all groups...")
            data_for_group_create = {}
            # Looking for redis hashset with group_name
            all_group_parameters = await self.redis.keys(
                self.prefix + group_schema.group_name + ":*"
            )
            self.logger.debug(
                msg=f"Get all group parameters {all_group_parameters}"
            )
            if not all_group_parameters:
                if group_schema.group_type_id == 1 and (
                    group_schema.column_filters or group_schema.elements
                ):
                    data: (
                        list[BaseModel],
                        bool,
                    ) = await self.app.store.grpc.search_get_statistic_from_mo(
                        group_schema=group_schema,
                        mo_ids=[el.entity_id for el in group_schema.elements],
                    )
                elif group_schema.group_type_id == 2:
                    data: (
                        list[BaseModel],
                        bool,
                    ) = await self.app.store.grpc.get_severity_processes(
                        group_schema=group_schema,
                        mo_ids=[el.entity_id for el in group_schema.elements],
                    )
                else:
                    raise TypeError("Incorrect group Type")
                if not data:
                    raise ValueError("Entities not found in Search")
                return await self.set_statistic_by_schema(
                    current_group=group_schema, data=data[0]
                )

            for parameter in all_group_parameters:
                data_from_redis: list = await self.redis.hvals(parameter)
                data_for_group_create.setdefault(
                    parameter.split(":")[2], {}
                ).update(
                    self._get_aggregated_data(
                        prm=parameter, data=data_from_redis
                    )
                )
            if not data_for_group_create.get("groupName", None):
                data_for_group_create |= {"groupName": group_schema.group_name}
            # Create GroupStat Model
            # group_stat: BaseModel = self.app.store.group_scheme[f"{group_model.tmo_id}"](**data_for_group_create)
            # return group_stat
        except MissingGreenlet:
            self.logger.warning(
                msg=f"No element in group: {group_schema.group_name}."
            )
            data_for_group_create = {}
            # group_stat: BaseModel = self.app.store.group_scheme[f"{group_model.tmo_id}"]()
        except TypeError:
            # self.logger.exception(ex)
            self.logger.info("Get statistic Error for data %s", group_schema)
            data_for_group_create = {
                "groupName": group_schema.group_name,
                "MO": {"tmo_id": group_schema.tmo_id},
            }
        except Exception as ex:
            self.logger.exception(ex)
            self.logger.warning(msg=f"Get statistic Error {type(ex)}: {ex}.)")
            self.logger.warning("Current group model: %s", group_schema)
            data_for_group_create = {
                "groupName": group_schema.group_name,
                "MO": {"tmo_id": group_schema.tmo_id},
            }
        group_stat: BaseModel = self.app.store.group_scheme[
            f"{group_schema.tmo_id}"
        ](**data_for_group_create)
        return group_stat

    async def get_statistic_by_schema_for_delete(
        self, group_schema: GroupSchema
    ) -> BaseModel:
        all_group_parameters = await self.redis.keys(
            self.prefix + group_schema.group_name + ":*"
        )

        data_for_group_create = {}
        if not all_group_parameters:
            # Generate empty statistic
            return self.generate_empty_statistic(group_schema=group_schema)
        for parameter in all_group_parameters:
            data_from_redis: list = await self.redis.hvals(parameter)
            data_for_group_create.setdefault(
                parameter.split(":")[2], {}
            ).update(
                self._get_aggregated_data(prm=parameter, data=data_from_redis)
            )
            if not data_for_group_create.get("groupName", None):
                data_for_group_create |= {"groupName": group_schema.group_name}
        group_stat: BaseModel = self.app.store.group_scheme[
            f"{group_schema.tmo_id}"
        ](**data_for_group_create)
        return group_stat

    async def remove_groups(self, group_names: list[str]) -> int:
        result = 0
        try:
            for group_name in group_names:
                keys = await self.redis.keys(f"{self.prefix}{group_name}:*")
                if keys:
                    result += await self.redis.delete(*keys)
            return result
        except Exception as ex:
            self.logger.warning("%s: %s.", type(ex), ex)
            raise

    async def delete_values(
        self, group_name: str, entity_ids: list[int]
    ) -> None:
        if entity_ids:
            try:
                all_group_parameters = await self.redis.keys(
                    f"{self.prefix}{group_name}:*"
                )
                # improve performance
                # https://stackoverflow.com/questions/21975228/
                # redis-python-how-to-delete-all-keys-according-to-a-specific-pattern-in-python
                for parameter in all_group_parameters:
                    await self.redis.hdel(parameter, *entity_ids)
            except Exception as ex:
                self.logger.warning(msg=f"Delete values {type(ex)}: {ex}.)")

    async def update_element_value(
        self,
        entity_id: int,
        tprm_id: int,
        new_value: T,
        list_groups: list[str],
        group: GroupModel,
    ):
        try:
            # Serialize value to correct type
            if group.group_type_id == 1:
                temp_model = self.app.store.group_scheme[str(group.tmo_id)](
                    **{str(tprm_id): new_value}
                )
            else:
                temp_model = self.app.store.group_scheme[str(group.tmo_id)](
                    **{"TPRM": {str(tprm_id): new_value}}
                )
            new_value = temp_model.model_dump(exclude_none=True)
            for group_name in list_groups:
                path, mapping = self._redis_param_builder(
                    group_name=group_name,
                    statistic_name="TPRM",
                    parameter_name=str(tprm_id),
                    parameter_value=new_value["TPRM"][str(tprm_id)],
                    entity_id=entity_id,
                )
                # Check if key exists need to return 1
                if await self.redis.hexists(path, str(entity_id)):
                    # Update information in redis
                    await self.redis.hset(name=path, mapping=mapping)
        except Exception as ex:
            self.logger.warning(
                msg=f"Redis add element Error: {type(ex)}: {ex}.)"
            )

    async def add_element_value(
        self,
        entity_id: int,
        tprm_id: int,
        new_value: T,
        list_groups: list[str],
        group: GroupModel,
    ):
        try:
            # Serialize value to correct type
            temp_model = self.app.store.group_scheme[str(group.tmo_id)](
                **{"TPRM": {str(tprm_id): new_value}}
            )
            new_value = temp_model.model_dump(exclude_none=True)
            for group_name in list_groups:
                path, mapping = self._redis_param_builder(
                    group_name=group_name,
                    statistic_name="TPRM",
                    parameter_name=str(tprm_id),
                    parameter_value=new_value["TPRM"][str(tprm_id)],
                    entity_id=entity_id,
                )

                await self.redis.hset(name=path, mapping=mapping)
                self.logger.info("Added element to Redis.")
        except Exception as ex:
            self.logger.warning(
                msg=f"Update redis element Error: {type(ex)}: {ex}.)"
            )

    async def remove_parameter(self, tprm_id: int, val_type: str):
        # Find all keys in redis by pattern
        _, group_from_redis = await self.redis.scan(
            cursor=0, match=f"*:TPRM:{val_type}:*:{tprm_id}", count=1000
        )
        try:
            for path in group_from_redis:
                await self.redis.delete(path)
        except Exception as ex:
            self.logger.warning(
                msg=f"Update redis element Error: {type(ex)}: {ex}.)"
            )

    def _redis_param_builder(
        self,
        group_name: str,
        statistic_name: str,
        parameter_name: str,
        parameter_value: T,
        entity_id: int,
    ) -> (str, dict[str, T]):
        # Strict mapping
        if mapper.get(parameter_name):
            if isinstance(parameter_value, bool):
                return (
                    f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                    f"{mapper[parameter_name]}:{parameter_name}",
                    {entity_id: int(parameter_value)},
                )
            elif isinstance(parameter_value, datetime):
                pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
                return (
                    f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                    f"{mapper[parameter_name]}:{parameter_name}",
                    {entity_id: parameter_value.strftime(pattern)},
                )
            elif parameter_value is None:
                return (
                    f"{self.prefix}{group_name}:{statistic_name}:None:{mapper[parameter_name]}"
                    f":{parameter_name}",
                    {entity_id: "None"},
                )
            else:
                return (
                    f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                    f"{mapper[parameter_name]}:{parameter_name}",
                    {entity_id: parameter_value},
                )
        # Mapping on type
        else:
            match parameter_value:
                case bool():
                    return (
                        f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                        f"frequency:{parameter_name}",
                        {entity_id: int(parameter_value)},
                    )
                case datetime():
                    pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
                    return (
                        f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                        f"maximum:{parameter_name}",
                        {entity_id: parameter_value.strftime(pattern)},
                    )
                case date():
                    pattern = "%Y-%m-%d"
                    return (
                        f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                        f"maximum:{parameter_name}",
                        {entity_id: parameter_value.strftime(pattern)},
                    )
                case int() | float():
                    return (
                        f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                        f"average:{parameter_name}",
                        {entity_id: parameter_value},
                    )
                case str():
                    return (
                        f"{self.prefix}{group_name}:{statistic_name}:{type(parameter_value).__name__}:"
                        f"frequency:{parameter_name}",
                        {entity_id: parameter_value},
                    )
                case _:
                    return (
                        f"{self.prefix}{group_name}:{statistic_name}:None:frequency:{parameter_name}",
                        {entity_id: "None"},
                    )

    def _get_aggregated_data(self, prm: str, data: list) -> dict:
        try:
            result: dict = {}
            *_, _, type_, agg, name = prm.split(":")
            if all([True if x in ["None", None] else False for x in data]):
                result |= {name: None}
                return result
            if agg == "frequency":
                result |= {name: max(set(data), key=data.count)}
            elif agg == "average":
                data_from_redis = [float(el) for el in data]
                if type_ == "int":
                    result |= {
                        name: int(sum(data_from_redis) / len(data_from_redis))
                    }
                elif type_ == "float":
                    result |= {
                        name: round(
                            sum(data_from_redis) / len(data_from_redis), 2
                        )
                    }
            elif agg == "maximum":
                result |= {name: max(data)}
            else:
                raise ValueError(f"Wrong aggregation data for: {prm}: {data}")
            return result
        except Exception as ex:
            self.logger.exception(ex)
            raise

    def generate_empty_statistic(self, group_schema: GroupSchema):
        raw_data = {}
        for model_name, field_info in self.app.store.group_scheme[
            f"{group_schema.tmo_id}"
        ].model_fields.items():
            if model_name == "groupName":
                raw_data[model_name] = ""
            else:
                model_class = field_info.annotation
                model_instance = model_class()
                raw_data[model_name] = model_instance.model_dump()
        # Add fake MO
        raw_data["MO"] = {}
        group_stat = self.app.store.group_scheme[f"{group_schema.tmo_id}"](
            **raw_data
        )
        return group_stat
