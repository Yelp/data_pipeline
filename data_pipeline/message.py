from __future__ import absolute_import

import time
from data_pipeline.fast_uuid import _FastUUID
from data_pipeline.message_type import MessageType


class Message(object):
    """Encapsulates a data pipeline message with metadata about the message.

    Validates metadata, but not the payload itself.

    Args:
        topic (str): Kafka topic to publish into
        schema_id (int): Identifies the schema used to encode the payload
        payload (bytes): Avro-encoded message - encoded with schema identified
            by `schema_id`.
        message_type (data_pipeline.message_type.MessageType): Identifies the
            nature of the message.
        uuid (bytes, optional): Globally-unique 16-byte identifier for the
            message.  A uuid4 will be generated automatically if this isn't
            provided.
        contains_pii (bool, optional): Indicates that the payload contains PII,
            so the clientlib can properly encrypt the data and mark it as
            sensitive, default to False.
        timestamp (int, optional): A unix timestamp for the message.  If this is
            not provided, a timestamp will be generated automatically.  If the
            message is coming directly from an upstream source, and the
            modification time is available in that source, it's appropriate to
            use that timestamp.  Otherwise, it's probably best to have the
            timestamp represent when the message was generated.
    """

    fast_uuid = _FastUUID()
    """UUID generator - this isn't a @cached_property so it can be serialized"""

    def __init__(
        self, topic, schema_id, payload, message_type, uuid=None, contains_pii=False,
        timestamp=None
    ):
        # The decision not to just pack the message to validate it is
        # intentional here.  We want to perform more sanity checks than avro
        # does, and in addition, this check is quite a bit faster than
        # serialization.  Finally, if we do it this way, we can lazily
        # serialize the payload in a subclass if necessary.
        if not isinstance(topic, str) or len(topic) == 0:
            raise ValueError("Topic must be a non-empty string")
        self.topic = topic

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
        """Prepare the message for packing

        Returns:
            dict: Dictionary that can be packed by
                :func:`data_pipeline.envelope.Envelope.pack`.
        """
        return {
            'uuid': self.uuid,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload': self.payload,
            'timestamp': self.timestamp
        }
