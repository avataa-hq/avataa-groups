import pickle

from google.protobuf import timestamp_pb2
from pydantic import BaseModel


def format_data_from_model_to_kafka_message_for_statistic(
    statistic: BaseModel, group_type: str
):
    new_statistic = dict()
    if statistic.MO:
        ts = timestamp_pb2.Timestamp()
        create_date = modified_date = None
        if statistic.MO.creation_date:
            create_date = ts.FromDatetime(statistic.MO.creation_date)
        if statistic.MO.modification_date:
            modified_date = ts.FromDatetime(statistic.MO.modification_date)

        new_statistic.update(
            id=statistic.MO.id,
            name=statistic.MO.name,
            pov=statistic.MO.pov,
            geometry=statistic.MO.geometry,
            active=statistic.MO.active,
            latitude=statistic.MO.latitude,
            longitude=statistic.MO.longitude,
            tmo_id=statistic.MO.tmo_id,
            p_id=statistic.MO.p_id,
            point_a_id=statistic.MO.point_a_id,
            point_b_id=statistic.MO.point_b_id,
            model=statistic.MO.model,
            version=statistic.MO.version,
            status=statistic.MO.status,
            creation_date=create_date,
            modification_date=modified_date,
            document_count=statistic.MO.document_count,
        )

    new_statistic["groupName"] = statistic.groupName
    new_statistic["group_type"] = group_type
    if statistic.TPRM:
        new_statistic["params"] = pickle.dumps(dict(statistic.TPRM)).hex()
    else:
        new_statistic["params"] = pickle.dumps(None).hex()
    return new_statistic
