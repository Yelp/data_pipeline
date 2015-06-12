# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.config import get_schema_cache
from data_pipeline.envelope import _AvroStringWriter
from data_pipeline.envelope import _get_avro_schema_object
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType


class LazyMessage(Message):
    """Encapsulates a data pipeline message with metadata about the message.

    The payload is lazily schematized using the schema represented by the
    schema id.

    For complete argument docs, see :class:`data_pipeline.message.Message`.

    Args:
        payload_data (dict): Contents of message.  Message will be lazily
            encoded with schema identified by `schema_id`.
        previous_payload_data (dict): Contents of message.  Message will be lazily
            encoded with schema identified by `schema_id`.  Required when
            message type is MessageType.update.  Disallowed otherwise.
            Defaults to None.
    """
    @property
    def payload(self):
        """Avro-encoded message - encoded with schema identified by `schema_id`.
        """
        schema = _get_avro_schema_object(
            get_schema_cache().get_schema(self.schema_id)
        )
        writer = _AvroStringWriter(schema=schema)
        return writer.encode(self._payload_data)

    @property
    def previous_payload(self):
        """Avro-encoded message - encoded with schema identified by
        `schema_id`.  Required when message type is `MessageType.update`.
        Disallowed otherwise.  Defaults to None.
        """
        if self.message_type == MessageType.update:
            schema = _get_avro_schema_object(
                get_schema_cache().get_schema(self.schema_id)
            )
            writer = _AvroStringWriter(schema=schema)
            return writer.encode(self._previous_payload_data)
        else:
            return None

    @property
    def _payload_data(self):
        return self._payload_data_repr

    @_payload_data.setter
    def _payload_data(self, payload_data):
        if not isinstance(payload_data, dict):
            raise ValueError("Payload data must be a dict containing data to serialize")
        self._payload_data_repr = payload_data

    @property
    def _previous_payload_data(self):
        return self._previous_payload_data_repr

    @_previous_payload_data.setter
    def _previous_payload_data(self, previous_payload_data):
        if self.message_type != MessageType.update and previous_payload_data is not None:
            raise ValueError("Previous payload data should only be set for updates")

        if (
            self.message_type == MessageType.update and
            not isinstance(previous_payload_data, dict)
        ):
            raise ValueError("Previous payload data must be a dict for updates")

        self._previous_payload_data_repr = previous_payload_data

    def __init__(
        self, topic, schema_id, payload_data, message_type,
        previous_payload_data=None, uuid=None, contains_pii=False,
        timestamp=None, upstream_position_info=None
    ):
        self.topic = topic
        self.schema_id = schema_id
        self._payload_data = payload_data
        self.message_type = message_type
        self._previous_payload_data = previous_payload_data
        self.uuid = uuid
        self.contains_pii = contains_pii
        self.timestamp = timestamp
        self.upstream_position_info = upstream_position_info
