from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GroupType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    object_group: _ClassVar[GroupType]
    process_group: _ClassVar[GroupType]
object_group: GroupType
process_group: GroupType

class GroupInfoCreate(_message.Message):
    __slots__ = ("group_name", "group_type", "tmo_id")
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    GROUP_TYPE_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    group_name: str
    group_type: str
    tmo_id: int
    def __init__(self, group_name: _Optional[str] = ..., group_type: _Optional[str] = ..., tmo_id: _Optional[int] = ...) -> None: ...

class Elements(_message.Message):
    __slots__ = ("group_name", "entity_id")
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    group_name: str
    entity_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, group_name: _Optional[str] = ..., entity_id: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestCreateGroup(_message.Message):
    __slots__ = ("group_info",)
    GROUP_INFO_FIELD_NUMBER: _ClassVar[int]
    group_info: _containers.RepeatedCompositeFieldContainer[GroupInfoCreate]
    def __init__(self, group_info: _Optional[_Iterable[_Union[GroupInfoCreate, _Mapping]]] = ...) -> None: ...

class RequestListGroupName(_message.Message):
    __slots__ = ("group_name",)
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    group_name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, group_name: _Optional[_Iterable[str]] = ...) -> None: ...

class RequestGroupByType(_message.Message):
    __slots__ = ("group_type",)
    GROUP_TYPE_FIELD_NUMBER: _ClassVar[int]
    group_type: GroupType
    def __init__(self, group_type: _Optional[_Union[GroupType, str]] = ...) -> None: ...

class ResponseGroupStatus(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: bool
    def __init__(self, response: bool = ...) -> None: ...

class RequestElements(_message.Message):
    __slots__ = ("elements",)
    ELEMENTS_FIELD_NUMBER: _ClassVar[int]
    elements: _containers.RepeatedCompositeFieldContainer[Elements]
    def __init__(self, elements: _Optional[_Iterable[_Union[Elements, _Mapping]]] = ...) -> None: ...

class ResponseListGroupName(_message.Message):
    __slots__ = ("group_name",)
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    group_name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, group_name: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseElements(_message.Message):
    __slots__ = ("elements",)
    ELEMENTS_FIELD_NUMBER: _ClassVar[int]
    elements: _containers.RepeatedCompositeFieldContainer[Elements]
    def __init__(self, elements: _Optional[_Iterable[_Union[Elements, _Mapping]]] = ...) -> None: ...

class RequestListGroupByTMOID(_message.Message):
    __slots__ = ("tmo_id",)
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class ResponseListGroupByTMOID(_message.Message):
    __slots__ = ("group_names",)
    GROUP_NAMES_FIELD_NUMBER: _ClassVar[int]
    group_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, group_names: _Optional[_Iterable[str]] = ...) -> None: ...

class RequestListMOIdsInSpecialGroup(_message.Message):
    __slots__ = ("group_name",)
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    group_name: str
    def __init__(self, group_name: _Optional[str] = ...) -> None: ...

class ResponseListMOIdsInSpecialGroup(_message.Message):
    __slots__ = ("entity_ids",)
    ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    entity_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, entity_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestGetGroupStatistic(_message.Message):
    __slots__ = ("group_name",)
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    group_name: str
    def __init__(self, group_name: _Optional[str] = ...) -> None: ...

class ResponseGetGroupStatistic(_message.Message):
    __slots__ = ("group_statistic",)
    GROUP_STATISTIC_FIELD_NUMBER: _ClassVar[int]
    group_statistic: str
    def __init__(self, group_statistic: _Optional[str] = ...) -> None: ...
