from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BoolValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bool
    def __init__(self, value: bool = ...) -> None: ...

class DeleteMOIdsRequest(_message.Message):
    __slots__ = ["mo_id"]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    mo_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_id: _Optional[_Iterable[int]] = ...) -> None: ...

class DeleteMOIdsResponse(_message.Message):
    __slots__ = ["deleted_quantity"]
    DELETED_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    deleted_quantity: int
    def __init__(self, deleted_quantity: _Optional[int] = ...) -> None: ...

class FloatValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: float
    def __init__(self, value: _Optional[float] = ...) -> None: ...

class GetAllMOWithParamsByTMOIdRequest(_message.Message):
    __slots__ = ["replace_links", "tmo_id"]
    REPLACE_LINKS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    replace_links: bool
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., replace_links: bool = ...) -> None: ...

class GetAllMOWithParamsByTMOIdResponse(_message.Message):
    __slots__ = ["mos_with_params"]
    MOS_WITH_PARAMS_FIELD_NUMBER: _ClassVar[int]
    mos_with_params: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mos_with_params: _Optional[_Iterable[str]] = ...) -> None: ...

class GetAllTMORequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetAllTMOResponse(_message.Message):
    __slots__ = ["tmo_info"]
    TMO_INFO_FIELD_NUMBER: _ClassVar[int]
    tmo_info: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tmo_info: _Optional[_Iterable[str]] = ...) -> None: ...

class GetMODataByIdsRequest(_message.Message):
    __slots__ = ["mo_ids"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class GetMODataByIdsResponse(_message.Message):
    __slots__ = ["list_of_mo"]
    LIST_OF_MO_FIELD_NUMBER: _ClassVar[int]
    list_of_mo: _containers.RepeatedCompositeFieldContainer[MOData]
    def __init__(self, list_of_mo: _Optional[_Iterable[_Union[MOData, _Mapping]]] = ...) -> None: ...

class GetPRMsByPRMIdsRequest(_message.Message):
    __slots__ = ["prm_ids"]
    PRM_IDS_FIELD_NUMBER: _ClassVar[int]
    prm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, prm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class GetPRMsByPRMIdsResponse(_message.Message):
    __slots__ = ["list_of_prm"]
    LIST_OF_PRM_FIELD_NUMBER: _ClassVar[int]
    list_of_prm: _containers.RepeatedCompositeFieldContainer[PRMMsgValueAsString]
    def __init__(self, list_of_prm: _Optional[_Iterable[_Union[PRMMsgValueAsString, _Mapping]]] = ...) -> None: ...

class InfoReply(_message.Message):
    __slots__ = ["mo_info"]
    class MoInfoEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: ValueOfDict
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[ValueOfDict, _Mapping]] = ...) -> None: ...
    MO_INFO_FIELD_NUMBER: _ClassVar[int]
    mo_info: _containers.MessageMap[int, ValueOfDict]
    def __init__(self, mo_info: _Optional[_Mapping[int, ValueOfDict]] = ...) -> None: ...

class InfoRequest(_message.Message):
    __slots__ = ["mo_id", "tprm_ids"]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_id: int
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_id: _Optional[int] = ..., tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class IntValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class MOData(_message.Message):
    __slots__ = ["id", "name", "tmo_id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    tmo_id: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tmo_id: _Optional[int] = ...) -> None: ...

class MOInfo(_message.Message):
    __slots__ = ["p_id", "tmo_id"]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    p_id: int
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., p_id: _Optional[int] = ...) -> None: ...

class MOInfoRequest(_message.Message):
    __slots__ = ["mo_ids"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class MOWithSpecialParametersRequest(_message.Message):
    __slots__ = ["tmo_id", "tprm_ids"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tmo_id: _Optional[int] = ..., tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class MOWithSpecialParametersResponse(_message.Message):
    __slots__ = ["mos_with_params"]
    MOS_WITH_PARAMS_FIELD_NUMBER: _ClassVar[int]
    mos_with_params: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mos_with_params: _Optional[_Iterable[str]] = ...) -> None: ...

class PRMMsgValueAsString(_message.Message):
    __slots__ = ["id", "tprm_id", "value", "version"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    id: int
    tprm_id: int
    value: str
    version: int
    def __init__(self, id: _Optional[int] = ..., tprm_id: _Optional[int] = ..., version: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class RequestForFilteredObjInfoByTMO(_message.Message):
    __slots__ = ["decoded_jwt", "mo_ids", "object_type_id", "order_by", "query_params"]
    DECODED_JWT_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    QUERY_PARAMS_FIELD_NUMBER: _ClassVar[int]
    decoded_jwt: str
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    object_type_id: int
    order_by: str
    query_params: str
    def __init__(self, object_type_id: _Optional[int] = ..., query_params: _Optional[str] = ..., order_by: _Optional[str] = ..., decoded_jwt: _Optional[str] = ..., mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestForFilteredObjSpecial(_message.Message):
    __slots__ = ["decoded_jwt", "mo_attrs", "mo_ids", "object_type_id", "only_ids", "order_by", "p_ids", "query_params", "tprm_ids"]
    DECODED_JWT_FIELD_NUMBER: _ClassVar[int]
    MO_ATTRS_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    ONLY_IDS_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    P_IDS_FIELD_NUMBER: _ClassVar[int]
    QUERY_PARAMS_FIELD_NUMBER: _ClassVar[int]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    decoded_jwt: str
    mo_attrs: _containers.RepeatedScalarFieldContainer[str]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    object_type_id: int
    only_ids: bool
    order_by: str
    p_ids: _containers.RepeatedScalarFieldContainer[int]
    query_params: str
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, object_type_id: _Optional[int] = ..., query_params: _Optional[str] = ..., order_by: _Optional[str] = ..., decoded_jwt: _Optional[str] = ..., mo_ids: _Optional[_Iterable[int]] = ..., p_ids: _Optional[_Iterable[int]] = ..., only_ids: bool = ..., tprm_ids: _Optional[_Iterable[int]] = ..., mo_attrs: _Optional[_Iterable[str]] = ...) -> None: ...

class RequestForObjInfoByTMO(_message.Message):
    __slots__ = ["mo_p_id", "object_type_id", "tprm_ids"]
    MO_P_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_p_id: int
    object_type_id: int
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, object_type_id: _Optional[int] = ..., tprm_ids: _Optional[_Iterable[int]] = ..., mo_p_id: _Optional[int] = ...) -> None: ...

class RequestGetAllRawPRMDataByTPRMId(_message.Message):
    __slots__ = ["tprm_id"]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    tprm_id: int
    def __init__(self, tprm_id: _Optional[int] = ...) -> None: ...

class RequestGetAllTPRMSByTMOId(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestGetMOsNamesByIds(_message.Message):
    __slots__ = ["mo_ids"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestGetTPRMAlldata(_message.Message):
    __slots__ = ["tprm_ids"]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestLevel(_message.Message):
    __slots__ = ["collect_data_for_tmos", "level_data", "level_tmo_id", "path_of_children_tmos"]
    COLLECT_DATA_FOR_TMOS_FIELD_NUMBER: _ClassVar[int]
    LEVEL_DATA_FIELD_NUMBER: _ClassVar[int]
    LEVEL_TMO_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_OF_CHILDREN_TMOS_FIELD_NUMBER: _ClassVar[int]
    collect_data_for_tmos: _containers.RepeatedScalarFieldContainer[int]
    level_data: _containers.RepeatedCompositeFieldContainer[RequestNode]
    level_tmo_id: int
    path_of_children_tmos: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, level_data: _Optional[_Iterable[_Union[RequestNode, _Mapping]]] = ..., level_tmo_id: _Optional[int] = ..., path_of_children_tmos: _Optional[_Iterable[int]] = ..., collect_data_for_tmos: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestListLevels(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[RequestLevel]
    def __init__(self, items: _Optional[_Iterable[_Union[RequestLevel, _Mapping]]] = ...) -> None: ...

class RequestMODetailsWithTPRMNames(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestNode(_message.Message):
    __slots__ = ["mo_ids", "node_id"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    node_id: str
    def __init__(self, node_id: _Optional[str] = ..., mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestObjWithParamsLimited(_message.Message):
    __slots__ = ["limit", "offset", "tmo_id", "tprm_names"]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_NAMES_FIELD_NUMBER: _ClassVar[int]
    limit: int
    offset: int
    tmo_id: int
    tprm_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tmo_id: _Optional[int] = ..., tprm_names: _Optional[_Iterable[str]] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class RequestSeverityMoId(_message.Message):
    __slots__ = ["mo_ids", "tmo_id"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestSeverityValues(_message.Message):
    __slots__ = ["dict_severities", "dict_tmo_with_mo_ids"]
    DICT_SEVERITIES_FIELD_NUMBER: _ClassVar[int]
    DICT_TMO_WITH_MO_IDS_FIELD_NUMBER: _ClassVar[int]
    dict_severities: str
    dict_tmo_with_mo_ids: str
    def __init__(self, dict_severities: _Optional[str] = ..., dict_tmo_with_mo_ids: _Optional[str] = ...) -> None: ...

class RequestTMOAttrsAndTypes(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ...) -> None: ...

class RequestTMOlifecycleByTMOidList(_message.Message):
    __slots__ = ["tmo_ids"]
    TMO_IDS_FIELD_NUMBER: _ClassVar[int]
    tmo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tmo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestTPRMData(_message.Message):
    __slots__ = ["tprm_ids"]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestTPRMIds(_message.Message):
    __slots__ = ["tprm_ids"]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tprm_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RequestTPRMNameToType(_message.Message):
    __slots__ = ["columns", "tmo_id"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    tmo_id: int
    def __init__(self, tmo_id: _Optional[int] = ..., columns: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseGetAllRawPRMDataByTPRMId(_message.Message):
    __slots__ = ["prms"]
    PRMS_FIELD_NUMBER: _ClassVar[int]
    prms: _containers.RepeatedCompositeFieldContainer[ResponseGetAllRawPRMDataByTPRMIdInnerMsg]
    def __init__(self, prms: _Optional[_Iterable[_Union[ResponseGetAllRawPRMDataByTPRMIdInnerMsg, _Mapping]]] = ...) -> None: ...

class ResponseGetAllRawPRMDataByTPRMIdInnerMsg(_message.Message):
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
    def __init__(self, id: _Optional[int] = ..., version: _Optional[int] = ..., tprm_id: _Optional[int] = ..., mo_id: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class ResponseGetAllTPRMSByTMOId(_message.Message):
    __slots__ = ["tprms_data"]
    TPRMS_DATA_FIELD_NUMBER: _ClassVar[int]
    tprms_data: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tprms_data: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseGetMOsNamesByIds(_message.Message):
    __slots__ = ["mo_names"]
    class MoNamesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    MO_NAMES_FIELD_NUMBER: _ClassVar[int]
    mo_names: _containers.ScalarMap[int, str]
    def __init__(self, mo_names: _Optional[_Mapping[int, str]] = ...) -> None: ...

class ResponseGetTPRMAlldata(_message.Message):
    __slots__ = ["tprms_data"]
    TPRMS_DATA_FIELD_NUMBER: _ClassVar[int]
    tprms_data: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tprms_data: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseListInt(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, values: _Optional[_Iterable[int]] = ...) -> None: ...

class ResponseListNodes(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ResponseNode]
    def __init__(self, items: _Optional[_Iterable[_Union[ResponseNode, _Mapping]]] = ...) -> None: ...

class ResponseMODetailsWithTPRMNames(_message.Message):
    __slots__ = ["column"]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    column: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, column: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseMOQuantityBySeverity(_message.Message):
    __slots__ = ["dict_mo_info"]
    DICT_MO_INFO_FIELD_NUMBER: _ClassVar[int]
    dict_mo_info: str
    def __init__(self, dict_mo_info: _Optional[str] = ...) -> None: ...

class ResponseMOdata(_message.Message):
    __slots__ = ["objects_with_parameters"]
    OBJECTS_WITH_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    objects_with_parameters: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, objects_with_parameters: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseMOdataSpecial(_message.Message):
    __slots__ = ["mo_ids", "pickle_mo_dataset"]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    PICKLE_MO_DATASET_FIELD_NUMBER: _ClassVar[int]
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    pickle_mo_dataset: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mo_ids: _Optional[_Iterable[int]] = ..., pickle_mo_dataset: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseNode(_message.Message):
    __slots__ = ["children_mo_ids", "node_id"]
    CHILDREN_MO_IDS_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    children_mo_ids: _containers.RepeatedScalarFieldContainer[int]
    node_id: str
    def __init__(self, node_id: _Optional[str] = ..., children_mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ResponseObjWithParamsLimited(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: str
    def __init__(self, data: _Optional[str] = ...) -> None: ...

class ResponseSeverityMoId(_message.Message):
    __slots__ = ["max_severity"]
    MAX_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    max_severity: int
    def __init__(self, max_severity: _Optional[int] = ...) -> None: ...

class ResponseTMOAttrsAndTypes(_message.Message):
    __slots__ = ["attrs"]
    ATTRS_FIELD_NUMBER: _ClassVar[int]
    attrs: _containers.RepeatedCompositeFieldContainer[TMOAttrAndType]
    def __init__(self, attrs: _Optional[_Iterable[_Union[TMOAttrAndType, _Mapping]]] = ...) -> None: ...

class ResponseTMOlifecycleByTMOidList(_message.Message):
    __slots__ = ["tmo_ids_with_lifecycle"]
    TMO_IDS_WITH_LIFECYCLE_FIELD_NUMBER: _ClassVar[int]
    tmo_ids_with_lifecycle: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tmo_ids_with_lifecycle: _Optional[_Iterable[int]] = ...) -> None: ...

class ResponseTPRMData(_message.Message):
    __slots__ = ["tprms_data"]
    TPRMS_DATA_FIELD_NUMBER: _ClassVar[int]
    tprms_data: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tprms_data: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseTPRMName(_message.Message):
    __slots__ = ["tprm_id", "tprm_name"]
    TPRM_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_NAME_FIELD_NUMBER: _ClassVar[int]
    tprm_id: int
    tprm_name: str
    def __init__(self, tprm_id: _Optional[int] = ..., tprm_name: _Optional[str] = ...) -> None: ...

class ResponseTPRMNameToType(_message.Message):
    __slots__ = ["mapper"]
    class MapperEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    MAPPER_FIELD_NUMBER: _ClassVar[int]
    mapper: _containers.ScalarMap[str, str]
    def __init__(self, mapper: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResponseTPRMNames(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ResponseTPRMName]
    def __init__(self, items: _Optional[_Iterable[_Union[ResponseTPRMName, _Mapping]]] = ...) -> None: ...

class ResponseWithObjInfoByTMO(_message.Message):
    __slots__ = ["mo_id", "p_id", "tprm_values"]
    class TprmValuesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    P_ID_FIELD_NUMBER: _ClassVar[int]
    TPRM_VALUES_FIELD_NUMBER: _ClassVar[int]
    mo_id: int
    p_id: int
    tprm_values: _containers.ScalarMap[int, str]
    def __init__(self, mo_id: _Optional[int] = ..., tprm_values: _Optional[_Mapping[int, str]] = ..., p_id: _Optional[int] = ...) -> None: ...

class StringValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class TMOAttrAndType(_message.Message):
    __slots__ = ["multiply", "name", "type"]
    MULTIPLY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    multiply: bool
    name: str
    type: str
    def __init__(self, name: _Optional[str] = ..., type: _Optional[str] = ..., multiply: bool = ...) -> None: ...

class TMOInfoRequest(_message.Message):
    __slots__ = ["tmo_id"]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    tmo_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tmo_id: _Optional[_Iterable[int]] = ...) -> None: ...

class TMOInfoResponse(_message.Message):
    __slots__ = ["tmo_info"]
    TMO_INFO_FIELD_NUMBER: _ClassVar[int]
    tmo_info: str
    def __init__(self, tmo_info: _Optional[str] = ...) -> None: ...

class ValueOfDict(_message.Message):
    __slots__ = ["mo_tprm_value"]
    MO_TPRM_VALUE_FIELD_NUMBER: _ClassVar[int]
    mo_tprm_value: _containers.RepeatedCompositeFieldContainer[_any_pb2.Any]
    def __init__(self, mo_tprm_value: _Optional[_Iterable[_Union[_any_pb2.Any, _Mapping]]] = ...) -> None: ...
