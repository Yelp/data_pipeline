# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

from data_pipeline._fast_uuid import FastUUID
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
        previous_payload (bytes): Avro-encoded message - encoded with schema
            identified by `schema_id`.  Required when message type is
            MessageType.update.  Disallowed otherwise.  Defaults to None.
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
            timestamp represent when the message was generated.  If the message
            is derived from an upstream data pipeline message, reuse the
            timestamp from that upstream message.

            Timestamp is used internally by the clientlib to monitor timings and
            other metadata about the data pipeline as a system.
            Consequently, there is no need to store information about when this
            message passed through individual systems in the message itself,
            as it is otherwise recorded.  See DATAPIPE-169 for details about
            monitoring.
        upstream_position_info (dict, optional): This dict must only contain
            primitive types.  It is not used internally by the data pipeline,
            so the content is left to the application.  The clientlib will
            track these objects and provide them back from the producer to
            identify the last message that was successfully published, both
            overall and per topic.
    """

    _fast_uuid = FastUUID()
    """UUID generator - this isn't a @cached_property so it can be serialized"""

    @property
    def topic(self):
        """The kafka topic the message should be published to."""
        return self._topic

    @topic.setter
    def topic(self, topic):
        if not isinstance(topic, str) or len(topic) == 0:
            raise ValueError("Topic must be a non-empty string")
        self._topic = topic

    @property
    def schema_id(self):
        """Integer identifying the schema used to encode the payload"""
        return self._schema_id

    @schema_id.setter
    def schema_id(self, schema_id):
        if not isinstance(schema_id, int):
            raise ValueError("Schema id should be an int")
        self._schema_id = schema_id

    @property
    def payload(self):
        """Avro-encoded message - encoded with schema identified by `schema_id`.
        """
        return self._payload

    @payload.setter
    def payload(self, payload):
        if len(payload) == 0:
            raise ValueError("Payload must exist")
        self._payload = payload

    @property
    def message_type(self):
        """Identifies the nature of the message."""
        return self._message_type

    @message_type.setter
    def message_type(self, message_type):
        if not isinstance(message_type, MessageType):
            message_types = [str(t) for t in MessageType]
            raise ValueError(
                "Message type should be one of %s" % ', '.join(message_types)
            )
        self._message_type = message_type

    @property
    def previous_payload(self):
        """Avro-encoded message - encoded with schema identified by
        `schema_id`.  Required when message type is `MessageType.update`.
        Disallowed otherwise.  Defaults to None.
        """
        return self._previous_payload

    @previous_payload.setter
    def previous_payload(self, previous_payload):
        if self.message_type != MessageType.update and previous_payload is not None:
            raise ValueError("Previous payload should only be set for updates")

        if self.message_type == MessageType.update and (
            previous_payload is None or len(previous_payload) == 0
        ):
            raise ValueError("Previous payload must exist for updates")

        self._previous_payload = previous_payload

    @property
    def uuid(self):
        """Globally-unique 16-byte identifier for the message.  A uuid4 will
        be generated automatically if this isn't provided.
        """
        return self._uuid

    @uuid.setter
    def uuid(self, uuid):
        if uuid is None:
            # UUID generation is expensive.  Using FastUUID instead of the built
            # in UUID methods increases Messages that can be instantiated per
            # second from ~25,000 to ~185,000.  Not generating UUIDs at all
            # increases the throughput further still to about 730,000 per
            # second.
            uuid = self._fast_uuid.uuid4()
        elif len(uuid) != 16:
            raise ValueError(
                "UUIDs should be exactly 16 bytes.  Conforming UUID's can be "
                "generated with `import uuid; uuid.uuid4().bytes`."
            )
        self._uuid = uuid

    @property
    def contains_pii(self):
        """Boolean indicating that the payload contains PII, so the clientlib
        can properly encrypt the data and mark it as sensitive.  The data
        pipeline consumer will automatically decrypt fields containing PII.
        This field shouldn't be used to indicate that a topic should be
        encrypted, because PII information will be used to indicate to various
        systems how to handle the data, in addition to automatic decryption.
        """
        return self._contains_pii

    @contains_pii.setter
    def contains_pii(self, contains_pii):
        if contains_pii:
            raise NotImplementedError(
                "Encryption of topics that contain PII has not yet been "
                "implemented.  See DATAPIPE-62 for details."
            )
        self._contains_pii = contains_pii

    @property
    def timestamp(self):
        """A unix timestamp for the message.  If this is not provided, a
        timestamp will be generated automatically.  If the message is coming
        directly from an upstream source, and the modification time is
        available in that source, it's appropriate to use that timestamp.
        Otherwise, it's probably best to have the timestamp represent when the
        message was generated.  If the message is derived from an upstream data
        pipeline message, the timestamp should be the timestamp from that
        upstream message.

        Timestamp is used internally by the clientlib to monitor timings and
        other metadata about the data pipeline as a system.
        Consequently, there is no need to store information about when this
        message passed through individual systems in the message itself,
        as it is otherwise recorded.  See DATAPIPE-169 for details about
        monitoring.
        """
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        if timestamp is None:
            timestamp = int(time.time())
        self._timestamp = timestamp

    @property
    def upstream_position_info(self):
        """The clientlib will track these objects and provide them back from
        the producer to identify the last message that was successfully
        published, both overall and per topic.
        """
        return self._upstream_position_info

    @upstream_position_info.setter
    def upstream_position_info(self, upstream_position_info):
        if upstream_position_info is not None and not isinstance(upstream_position_info, dict):
            raise ValueError("upstream_position_info should be None or a dict")
        self._upstream_position_info = upstream_position_info

    def __init__(
        self, topic, schema_id, payload, message_type, previous_payload=None,
        uuid=None, contains_pii=False, timestamp=None, upstream_position_info=None
    ):
        # The decision not to just pack the message to validate it is
        # intentional here.  We want to perform more sanity checks than avro
        # does, and in addition, this check is quite a bit faster than
        # serialization.  Finally, if we do it this way, we can lazily
        # serialize the payload in a subclass if necessary.
        self.topic = topic
        self.schema_id = schema_id
        self.payload = payload
        self.message_type = message_type
        self.previous_payload = previous_payload
        self.uuid = uuid
        self.contains_pii = contains_pii
        self.timestamp = timestamp
        self.upstream_position_info = upstream_position_info
