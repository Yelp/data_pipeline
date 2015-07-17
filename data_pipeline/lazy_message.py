# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

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
        dry_run (boolean): When set to True, LazyMessage will return a string
            representation of the payload and previous payload, instead of
            the avro encoded message.  This is to avoid loading the schema from
            the schema store.  Defaults to False.
    """
    @property
    def payload(self):
        """Avro-encoded message - encoded with schema identified by `schema_id`.
        """
        if self._payload is None:
            self._payload = self._encode_data(self.payload_data)
        return self._payload

    @property
    def previous_payload(self):
        """Avro-encoded message - encoded with schema identified by
        `schema_id`.  Required when message type is `MessageType.update`.
        Disallowed otherwise.  Defaults to None.
        """
        if self.message_type == MessageType.update:
            if self._previous_payload is None:
                self._previous_payload = self._encode_data(self._previous_payload_data)

            return self._previous_payload
        else:
            raise ValueError("Previous payload data should only be set for updates")

    @property
    def payload_data(self):
        return self._payload_data

    @payload_data.setter
    def payload_data(self, payload_data):
        if not isinstance(payload_data, dict):
            raise ValueError("Payload data must be a dict containing data to serialize")
        self._payload_data = payload_data
        self._payload = None

    @property
    def previous_payload_data(self):
        return self._previous_payload_data

    @previous_payload_data.setter
    def previous_payload_data(self, previous_payload_data):
        if self.message_type != MessageType.update and previous_payload_data is not None:
            raise ValueError("Previous payload data should only be set for updates")

        if (
            self.message_type == MessageType.update and
            not isinstance(previous_payload_data, dict)
        ):
            raise ValueError("Previous payload data must be a dict for updates")

        self._previous_payload_data = previous_payload_data
        self._previous_payload = None

    @property
    def dry_run(self):
        return self.dry_run

    def _encode_data(self, data):
        """Encodes data, returning a repr in dry_run mode"""
        if self.dry_run:
            return repr(data)

        return self._avro_string_writer.encode(message_avro_representation=data)

    def __init__(
        self, topic, schema_id, payload_data, message_type,
        previous_payload_data=None, uuid=None, contains_pii=False,
        timestamp=None, upstream_position_info=None, dry_run=False
    ):
        self.dry_run = dry_run

        # payload and previous_payload are lazily constructed only on request
        self._payload = None
        self._previous_payload = None

        self.topic = topic
        self.schema_id = schema_id
        self.payload_data = payload_data
        self.message_type = message_type
        self.previous_payload_data = previous_payload_data
        self.uuid = uuid
        self.contains_pii = contains_pii
        self.timestamp = timestamp
        self.upstream_position_info = upstream_position_info
