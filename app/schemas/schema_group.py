from datetime import datetime
from typing import Annotated, Any, Hashable, TypeVar

from pydantic import AfterValidator, BaseModel
from pydantic_core import PydanticCustomError

from schemas.schema_element import ElementSchema
from schemas.schema_group_type import GroupTypeSchema

T = TypeVar("T", bound=Hashable)


def _validate_unique_list(v: list[T]) -> list[T]:
    if len(v) != len(set(v)):
        raise PydanticCustomError("unique_list", "List must be unique")
    return v


UniqueList = Annotated[list[T], AfterValidator(_validate_unique_list)]


class GroupForKafka(BaseModel):
    group_name: str
    group_type: str
    entity_ids: list[int]
    tmo_id: int


class InputGroupFromUser(BaseModel):
    group_name: str
    group_type: str
    tmo_id: int
    columnFilters: list[dict[str, Any]] | None = None
    ranges_object: dict | None = None
    is_aggregate: bool = False
    min_qnt: int | None = None
    group_template_id: int | None = None


class GroupBase(BaseModel):
    group_name: str
    group_type_id: int
    group_process_instance_key: int | None = None
    tmo_id: int
    is_valid: bool | None = None
    column_filters: list[dict[str, Any]] | None = None
    ranges_object: dict | None = None
    is_aggregate: bool | None = None
    min_qnt: int | None = None
    group_template_id: int | None = None


class TMOSchema(BaseModel):
    p_id: int | None = None
    name: str | None = None
    tmo_id: int | None = None
    icon: str | None = None
    description: str | None = None
    latitude: int | None = None
    longitude: int | None = None
    virtual: bool | None = None
    created_by: str | None = None
    modified_by: str | None = None
    creation_date: datetime | None = None
    modification_date: datetime | None = None
    primary: list | None = None
    global_uniqueness: bool | None = None
    status: int | None = None
    version: int | None = None
    lifecycle_process_definition: str | None = None
    # geometry_type: int
    materialize: bool | None = None
    severity_id: int | None = None


class CamundaSchema(BaseModel):
    startDate: datetime | None = None
    state: str | None = None
    endDate: datetime | None = None
    processDefinitionKey: str | None = None
    processDefinitionVersion: int | None = None
    processInstanceId: int | None = None


class GroupInDB(GroupBase):
    id: int
    elements: list[ElementSchema]
    group_type: GroupTypeSchema | None = None

    class Config:
        from_attributes = True


class GroupSchema(GroupInDB):
    pass


class GroupResponse(BaseModel):
    total: int
    groups: list[GroupSchema]


class GroupSchemaDelete(BaseModel):
    group_ids: UniqueList[int]
