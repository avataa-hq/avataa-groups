from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Statistic(_message.Message):
    __slots__ = ("id", "name", "groupName", "pov", "geometry", "active", "latitude", "longitude", "tmo_id", "p_id", "point_a_id", "point_b_id", "model", "version", "status", "creation_date", "modification_date", "params", "document_count", "group_type")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    GROUPNAME_FIELD_NUMBER: _ClassVar[int]
    POV_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_A_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_B_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    MODIFICATION_DATE_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    GROUP_TYPE_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    groupName: str
    pov: _struct_pb2.Struct
    geometry: _struct_pb2.Struct
    active: bool
    latitude: float
    longitude: float
    tmo_id: int
    p_id: int
    point_a_id: int
    point_b_id: int
    model: str
    version: int
    status: str
    creation_date: _timestamp_pb2.Timestamp
    modification_date: _timestamp_pb2.Timestamp
    params: str
    document_count: int
    group_type: str
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., groupName: _Optional[str] = ..., pov: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., geometry: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., active: bool = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., tmo_id: _Optional[int] = ..., p_id: _Optional[int] = ..., point_a_id: _Optional[int] = ..., point_b_id: _Optional[int] = ..., model: _Optional[str] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., creation_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modification_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., params: _Optional[str] = ..., document_count: _Optional[int] = ..., group_type: _Optional[str] = ...) -> None: ...

class DeleteStatistic(_message.Message):
    __slots__ = ("groupName",)
    GROUPNAME_FIELD_NUMBER: _ClassVar[int]
    groupName: str
    def __init__(self, groupName: _Optional[str] = ...) -> None: ...
