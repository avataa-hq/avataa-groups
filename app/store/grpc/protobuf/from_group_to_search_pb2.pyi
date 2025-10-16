from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestGetProcesses(_message.Message):
    __slots__ = ("user_data", "tmo_id", "filters_list", "sort", "find_by_value", "with_groups", "limit", "ranges_object")
    USER_DATA_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    FIND_BY_VALUE_FIELD_NUMBER: _ClassVar[int]
    WITH_GROUPS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    RANGES_OBJECT_FIELD_NUMBER: _ClassVar[int]
    user_data: str
    tmo_id: int
    filters_list: str
    sort: str
    find_by_value: str
    with_groups: bool
    limit: str
    ranges_object: str
    def __init__(self, user_data: _Optional[str] = ..., tmo_id: _Optional[int] = ..., filters_list: _Optional[str] = ..., sort: _Optional[str] = ..., find_by_value: _Optional[str] = ..., with_groups: bool = ..., limit: _Optional[str] = ..., ranges_object: _Optional[str] = ...) -> None: ...

class RequestGetProcessesGroups(_message.Message):
    __slots__ = ("user_data", "tmo_id", "filters_list", "sort", "find_by_value", "with_groups", "limit", "ranges_object", "group_by", "min_group_qty")
    USER_DATA_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    FIND_BY_VALUE_FIELD_NUMBER: _ClassVar[int]
    WITH_GROUPS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    RANGES_OBJECT_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    MIN_GROUP_QTY_FIELD_NUMBER: _ClassVar[int]
    user_data: str
    tmo_id: int
    filters_list: str
    sort: str
    find_by_value: str
    with_groups: bool
    limit: str
    ranges_object: str
    group_by: str
    min_group_qty: int
    def __init__(self, user_data: _Optional[str] = ..., tmo_id: _Optional[int] = ..., filters_list: _Optional[str] = ..., sort: _Optional[str] = ..., find_by_value: _Optional[str] = ..., with_groups: bool = ..., limit: _Optional[str] = ..., ranges_object: _Optional[str] = ..., group_by: _Optional[str] = ..., min_group_qty: _Optional[int] = ...) -> None: ...

class ResponseGetProcesses(_message.Message):
    __slots__ = ("mo",)
    MO_FIELD_NUMBER: _ClassVar[int]
    mo: str
    def __init__(self, mo: _Optional[str] = ...) -> None: ...

class ProcessGroupKey(_message.Message):
    __slots__ = ("grouped_by", "grouping_value")
    GROUPED_BY_FIELD_NUMBER: _ClassVar[int]
    GROUPING_VALUE_FIELD_NUMBER: _ClassVar[int]
    grouped_by: str
    grouping_value: str
    def __init__(self, grouped_by: _Optional[str] = ..., grouping_value: _Optional[str] = ...) -> None: ...

class ProcessesGroupItem(_message.Message):
    __slots__ = ("group", "quantity")
    GROUP_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    group: _containers.RepeatedCompositeFieldContainer[ProcessGroupKey]
    quantity: int
    def __init__(self, group: _Optional[_Iterable[_Union[ProcessGroupKey, _Mapping]]] = ..., quantity: _Optional[int] = ...) -> None: ...

class ResponseProcessesGroups(_message.Message):
    __slots__ = ("item",)
    ITEM_FIELD_NUMBER: _ClassVar[int]
    item: ProcessesGroupItem
    def __init__(self, item: _Optional[_Union[ProcessesGroupItem, _Mapping]] = ...) -> None: ...

class RequestGetMOsByFilters(_message.Message):
    __slots__ = ("tmo_id", "filters_list", "with_groups")
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    WITH_GROUPS_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    filters_list: str
    with_groups: bool
    def __init__(self, tmo_id: _Optional[int] = ..., filters_list: _Optional[str] = ..., with_groups: bool = ...) -> None: ...

class ResponseGetMOsByFilters(_message.Message):
    __slots__ = ("mos",)
    MOS_FIELD_NUMBER: _ClassVar[int]
    mos: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mos: _Optional[_Iterable[str]] = ...) -> None: ...
