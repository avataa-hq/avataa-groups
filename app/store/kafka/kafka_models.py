from enum import StrEnum
from typing import TypeVar

import store.kafka.protobuf.group_pb2 as group
import store.kafka.protobuf.inventory_instances_pb2 as inventory_instances_pb2
import store.kafka.protobuf.kafka_process_pb2 as process


class KafkaType(StrEnum):
    AIOKAFKA = "AIOKafka"
    KAFKA_PYTHON = "KafkaPython"
    CONFLUENT_KAFKA = "ConfluentKafka"


class KafkaConfigType(StrEnum):
    CONSUMER = "Consumer"
    PRODUCER = "Producer"


kafka_protobuf_message_type = {
    "MO": inventory_instances_pb2.ListMO,
    "TMO": inventory_instances_pb2.ListTMO,
    "TPRM": inventory_instances_pb2.ListTPRM,
    "PRM": inventory_instances_pb2.ListPRM,
    "Process": process.Process,
}

kafka_protobuf_message_action = {
    "CREATED": "created",
    "UPDATED": "updated",
    "DELETED": "deleted",
}
T = TypeVar(
    "T",
    inventory_instances_pb2.MO,
    inventory_instances_pb2.TMO,
    inventory_instances_pb2.TPRM,
    inventory_instances_pb2.PRM,
    group.GROUP,
)
