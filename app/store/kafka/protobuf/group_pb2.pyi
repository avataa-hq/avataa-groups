from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GroupType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    object_group: _ClassVar[GroupType]
    process_group: _ClassVar[GroupType]
object_group: GroupType
process_group: GroupType

class GROUP(_message.Message):
    __slots__ = ("group_name", "entity_id", "group_type", "tmo_id")
    GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_TYPE_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    group_name: str
    entity_id: _containers.RepeatedScalarFieldContainer[int]
    group_type: str
    tmo_id: int
    def __init__(self, group_name: _Optional[str] = ..., entity_id: _Optional[_Iterable[int]] = ..., group_type: _Optional[str] = ..., tmo_id: _Optional[int] = ...) -> None: ...
