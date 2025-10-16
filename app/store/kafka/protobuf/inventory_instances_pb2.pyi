from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListMO(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[MO]
    def __init__(self, objects: _Optional[_Iterable[_Union[MO, _Mapping]]] = ...) -> None: ...

class ListPRM(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[PRM]
    def __init__(self, objects: _Optional[_Iterable[_Union[PRM, _Mapping]]] = ...) -> None: ...

class ListTMO(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[TMO]
    def __init__(self, objects: _Optional[_Iterable[_Union[TMO, _Mapping]]] = ...) -> None: ...

class ListTPRM(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[TPRM]
    def __init__(self, objects: _Optional[_Iterable[_Union[TPRM, _Mapping]]] = ...) -> None: ...

class MO(_message.Message):
    __slots__ = ["active", "creation_date", "description", "document_count", "geometry", "id", "label", "latitude", "longitude", "model", "modification_date", "name", "p_id", "point_a_id", "point_b_id", "pov", "status", "tmo_id", "version"]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    MODIFICATION_DATE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    POINT_A_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_B_ID_FIELD_NUMBER: _ClassVar[int]
    POV_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    active: bool
    creation_date: _timestamp_pb2.Timestamp
    description: str
    document_count: int
    geometry: _struct_pb2.Struct
    id: int
    label: str
    latitude: float
    longitude: float
    model: str
    modification_date: _timestamp_pb2.Timestamp
    name: str
    p_id: int
    point_a_id: int
    point_b_id: int
    pov: _struct_pb2.Struct
    status: str
    tmo_id: int
    version: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., pov: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., geometry: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., active: bool = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., tmo_id: _Optional[int] = ..., p_id: _Optional[int] = ..., point_a_id: _Optional[int] = ..., point_b_id: _Optional[int] = ..., model: _Optional[str] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., creation_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modification_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., document_count: _Optional[int] = ..., label: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class PRM(_message.Message):
    __slots__ = ["id", "mo_id", "tprm_id", "value", "version"]
    ID_FIELD_NUMBER: _ClassVar[int]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    id: int
    mo_id: int
    tprm_id: int
    value: str
    version: int
    def __init__(self, id: _Optional[int] = ..., value: _Optional[str] = ..., tprm_id: _Optional[int] = ..., mo_id: _Optional[int] = ..., version: _Optional[int] = ...) -> None: ...

class TMO(_message.Message):
    __slots__ = ["created_by", "creation_date", "description", "geometry_type", "global_uniqueness", "icon", "id", "label", "latitude", "lifecycle_process_definition", "longitude", "materialize", "minimize", "modification_date", "modified_by", "name", "p_id", "points_constraint_by_tmo", "primary", "severity_id", "status", "version", "virtual"]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_UNIQUENESS_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_PROCESS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    MATERIALIZE_FIELD_NUMBER: _ClassVar[int]
    MINIMIZE_FIELD_NUMBER: _ClassVar[int]
    MODIFICATION_DATE_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_BY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    POINTS_CONSTRAINT_BY_TMO_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    VIRTUAL_FIELD_NUMBER: _ClassVar[int]
    created_by: str
    creation_date: _timestamp_pb2.Timestamp
    description: str
    geometry_type: str
    global_uniqueness: bool
    icon: str
    id: int
    label: _containers.RepeatedScalarFieldContainer[int]
    latitude: int
    lifecycle_process_definition: str
    longitude: int
    materialize: bool
    minimize: bool
    modification_date: _timestamp_pb2.Timestamp
    modified_by: str
    name: str
    p_id: int
    points_constraint_by_tmo: _containers.RepeatedScalarFieldContainer[int]
    primary: _containers.RepeatedScalarFieldContainer[int]
    severity_id: int
    status: int
    version: int
    virtual: bool
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., p_id: _Optional[int] = ..., icon: _Optional[str] = ..., description: _Optional[str] = ..., virtual: bool = ..., global_uniqueness: bool = ..., latitude: _Optional[int] = ..., longitude: _Optional[int] = ..., created_by: _Optional[str] = ..., modified_by: _Optional[str] = ..., creation_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modification_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., primary: _Optional[_Iterable[int]] = ..., version: _Optional[int] = ..., status: _Optional[int] = ..., lifecycle_process_definition: _Optional[str] = ..., materialize: bool = ..., severity_id: _Optional[int] = ..., geometry_type: _Optional[str] = ..., points_constraint_by_tmo: _Optional[_Iterable[int]] = ..., minimize: bool = ..., label: _Optional[_Iterable[int]] = ...) -> None: ...

class TPRM(_message.Message):
    __slots__ = ["constraint", "created_by", "creation_date", "description", "field_value", "group", "id", "modification_date", "modified_by", "multiple", "name", "prm_link_filter", "required", "returnable", "tmo_id", "val_type", "version"]
    CONSTRAINT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    FIELD_VALUE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    MODIFICATION_DATE_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_BY_FIELD_NUMBER: _ClassVar[int]
    MULTIPLE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRM_LINK_FILTER_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    RETURNABLE_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    VAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    constraint: str
    created_by: str
    creation_date: _timestamp_pb2.Timestamp
    description: str
    field_value: str
    group: str
    id: int
    modification_date: _timestamp_pb2.Timestamp
    modified_by: str
    multiple: bool
    name: str
    prm_link_filter: str
    required: bool
    returnable: bool
    tmo_id: int
    val_type: str
    version: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., val_type: _Optional[str] = ..., multiple: bool = ..., required: bool = ..., returnable: bool = ..., constraint: _Optional[str] = ..., prm_link_filter: _Optional[str] = ..., group: _Optional[str] = ..., tmo_id: _Optional[int] = ..., created_by: _Optional[str] = ..., modified_by: _Optional[str] = ..., creation_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modification_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., version: _Optional[int] = ..., field_value: _Optional[str] = ...) -> None: ...
