from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ByHierarchyInput(_message.Message):
    __slots__ = ("hierarchy_id", "parent_id", "tmo_id", "filters")
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    hierarchy_id: int
    parent_id: str
    tmo_id: int
    filters: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, hierarchy_id: _Optional[int] = ..., parent_id: _Optional[str] = ..., tmo_id: _Optional[int] = ..., filters: _Optional[_Iterable[str]] = ...) -> None: ...

class FilterInput(_message.Message):
    __slots__ = ("filters",)
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    filters: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, filters: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseSeverityItem(_message.Message):
    __slots__ = ("filter_name", "count", "max_severity")
    FILTER_NAME_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    filter_name: str
    count: int
    max_severity: int
    def __init__(self, filter_name: _Optional[str] = ..., count: _Optional[int] = ..., max_severity: _Optional[int] = ...) -> None: ...

class ListResponseSeverity(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ResponseSeverityItem]
    def __init__(self, items: _Optional[_Iterable[_Union[ResponseSeverityItem, _Mapping]]] = ...) -> None: ...

class ByRangesInput(_message.Message):
    __slots__ = ("filters_list", "ranges_object", "tmo_ids", "mo_ids", "find_by_value")
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    RANGES_OBJECT_FIELD_NUMBER: _ClassVar[int]
    TMO_IDS_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    FIND_BY_VALUE_FIELD_NUMBER: _ClassVar[int]
    filters_list: _containers.RepeatedScalarFieldContainer[str]
    ranges_object: str
    tmo_ids: _containers.RepeatedScalarFieldContainer[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    find_by_value: str
    def __init__(self, filters_list: _Optional[_Iterable[str]] = ..., ranges_object: _Optional[str] = ..., tmo_ids: _Optional[_Iterable[int]] = ..., mo_ids: _Optional[_Iterable[int]] = ..., find_by_value: _Optional[str] = ...) -> None: ...

class ProcessesInput(_message.Message):
    __slots__ = ("filters_list", "ranges_object", "tmo_ids", "mo_ids", "sort", "limit", "find_by_value", "with_groups")
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    RANGES_OBJECT_FIELD_NUMBER: _ClassVar[int]
    TMO_IDS_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    FIND_BY_VALUE_FIELD_NUMBER: _ClassVar[int]
    WITH_GROUPS_FIELD_NUMBER: _ClassVar[int]
    filters_list: _containers.RepeatedScalarFieldContainer[str]
    ranges_object: str
    tmo_ids: _containers.RepeatedScalarFieldContainer[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    sort: _containers.RepeatedScalarFieldContainer[str]
    limit: str
    find_by_value: str
    with_groups: bool
    def __init__(self, filters_list: _Optional[_Iterable[str]] = ..., ranges_object: _Optional[str] = ..., tmo_ids: _Optional[_Iterable[int]] = ..., mo_ids: _Optional[_Iterable[int]] = ..., sort: _Optional[_Iterable[str]] = ..., limit: _Optional[str] = ..., find_by_value: _Optional[str] = ..., with_groups: bool = ...) -> None: ...

class ProcessesResponse(_message.Message):
    __slots__ = ("rows", "total_count")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedScalarFieldContainer[str]
    total_count: int
    def __init__(self, rows: _Optional[_Iterable[str]] = ..., total_count: _Optional[int] = ...) -> None: ...

class GetProcessInstanceForSpecialTMORequest(_message.Message):
    __slots__ = ("tmo_id",)
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class GetProcessInstanceForSpecialTMOItem(_message.Message):
    __slots__ = ("id", "processDefinitionKey", "processDefinitionVersion", "processDefinitionId", "processInstanceId", "startDate", "endDate", "duration", "state")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSDEFINITIONKEY_FIELD_NUMBER: _ClassVar[int]
    PROCESSDEFINITIONVERSION_FIELD_NUMBER: _ClassVar[int]
    PROCESSDEFINITIONID_FIELD_NUMBER: _ClassVar[int]
    PROCESSINSTANCEID_FIELD_NUMBER: _ClassVar[int]
    STARTDATE_FIELD_NUMBER: _ClassVar[int]
    ENDDATE_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    id: int
    processDefinitionKey: str
    processDefinitionVersion: int
    processDefinitionId: int
    processInstanceId: int
    startDate: str
    endDate: str
    duration: int
    state: str
    def __init__(self, id: _Optional[int] = ..., processDefinitionKey: _Optional[str] = ..., processDefinitionVersion: _Optional[int] = ..., processDefinitionId: _Optional[int] = ..., processInstanceId: _Optional[int] = ..., startDate: _Optional[str] = ..., endDate: _Optional[str] = ..., duration: _Optional[int] = ..., state: _Optional[str] = ...) -> None: ...

class GetProcessInstanceForSpecialTMOResponse(_message.Message):
    __slots__ = ("pr_inst_data",)
    PR_INST_DATA_FIELD_NUMBER: _ClassVar[int]
    pr_inst_data: _containers.RepeatedCompositeFieldContainer[GetProcessInstanceForSpecialTMOItem]
    def __init__(self, pr_inst_data: _Optional[_Iterable[_Union[GetProcessInstanceForSpecialTMOItem, _Mapping]]] = ...) -> None: ...
