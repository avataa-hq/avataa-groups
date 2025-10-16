from typing import Any

from confluent_kafka import cimpl
from google.protobuf import json_format
from google.protobuf.internal.containers import RepeatedScalarFieldContainer
from google.protobuf.internal.well_known_types import Struct, Timestamp


def from_struct_to_dict(value: Struct):
    """Converts Struct to python dict and returns it"""
    return json_format.MessageToDict(value)


def from_proto_timestamp_to_dict(value: Timestamp):
    """Converts proto Timestamp to python str and returns it"""
    return json_format.MessageToDict(value).split("Z")[0]


def from_repeated_scalar_field_container_to_list(
    value: RepeatedScalarFieldContainer,
):
    """Converts proto Timestamp to python str and returns it"""
    return list(value)


PROTO_TYPES_SERIALIZERS = {
    "Struct": from_struct_to_dict,
    "Timestamp": from_proto_timestamp_to_dict,
    "RepeatedScalarFieldContainer": from_repeated_scalar_field_container_to_list,
    "RepeatedScalarContainer": from_repeated_scalar_field_container_to_list,
}


def __msg_f_serializer(value: Any):
    """Returns serialized proto msg field value into python type"""
    serializer = PROTO_TYPES_SERIALIZERS.get(type(value).__name__)
    if serializer:
        return serializer(value)
    else:
        return value


def protobuf_kafka_msg_to_dict(
    msg: cimpl.Message, including_default_value_fields: bool
) -> dict:
    """Serialises protobuf.message.Message into python dict and returns it"""

    message_as_dict = dict()
    if including_default_value_fields is False:
        message_as_dict["objects"] = [
            {
                field.name: __msg_f_serializer(value)
                for field, value in item.ListFields()
            }
            for item in msg.objects
        ]
    else:
        message_as_dict["objects"] = [
            {
                field: __msg_f_serializer(getattr(item, field))
                for field in item.DESCRIPTOR.fields_by_name.keys()
            }
            for item in msg.objects
        ]
    return message_as_dict
