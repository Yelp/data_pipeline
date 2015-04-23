from __future__ import absolute_import

import time
from data_pipeline.fast_uuid import FastUUID
from data_pipeline.message_type import MessageType
from data_pipeline.message_type import ProtectedMessageType


class Message(object):
    fast_uuid = FastUUID()

    def __init__(
        self, schema_id, payload, message_type, uuid=None, contains_pii=False,
        timestamp=None
    ):
        if not isinstance(schema_id, int):
            raise ValueError("Schema id should be an int")
        self.schema_id = schema_id

        if len(payload) == 0:
            raise ValueError("Payload must exist")
        self.payload = payload

        if not (isinstance(message_type, MessageType)):
            message_types = ["MessageType.%s" % t.name for t in MessageType]
            raise ValueError(
                "Message type should be one of %s" % ', '.join(message_types)
            )
        self.message_type = message_type

        if uuid is None:
            # UUID generation is expensive.  Using FastUUID instead of the built
            # in UUID methods increases Messages that can be instantiated per
            # second from ~25,000 to ~185,000.  Not generating UUIDs at all
            # increases the throughput further still to about 730,000 per
            # second.
            uuid = self.fast_uuid.uuid4()
        elif len(uuid) != 16:
            raise ValueError(
                "UUIDs should be exactly 16 bytes.  Conforming UUID's can be "
                "generated with `import uuid; uuid.uuid4().bytes`."
            )
        self.uuid = uuid

        if contains_pii:
            raise NotImplementedError(
                "Encryption of topics that contain PII has not yet been "
                "implemented.  See DATAPIPE-62 for details."
            )

        if timestamp is None:
            timestamp = int(time.time())
        self.timestamp = timestamp

    def get_avro_repr(self):
        return {
            'uuid': self.uuid,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload': self.payload,
            'timestamp': self.timestamp
        }
