# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.message import Message
from data_pipeline.message import UpdateMessage
from data_pipeline.message_type import MessageType


class LazyMessage(Message):
    """Encapsulates a data pipeline message with metadata about the message.

    The payload is lazily schematized using the schema represented by the
    schema id.

    For complete argument docs, see :class:`data_pipeline.message.Message`.

    This class has exact same functionality as :class:`data_pipeline.message.Message`
    except it provides a different constructor, which takes dictionary payload
    instead of bytes.

    Args:
        payload_data (dict): Contents of message.  Message will be lazily
            encoded with schema identified by `schema_id`.
        dry_run (boolean): When set to True, LazyMessage will return a string
            representation of the payload and previous payload, instead of
            the avro encoded message.  This is to avoid loading the schema from
            the schema store.  Defaults to False.
    """

    def __init__(
        self,
        topic,
        schema_id,
        payload_data,
        uuid=None,
        contains_pii=False,
        timestamp=None,
        upstream_position_info=None,
        kafka_position_info=None,
        dry_run=False
    ):
        self.dry_run = dry_run

        # payload and previous_payload are lazily constructed only on request
        self._payload = None
        self._previous_payload = None

        self.topic = topic
        self.schema_id = schema_id
        self.payload_data = payload_data
        self.uuid = uuid
        self.contains_pii = contains_pii
        self.timestamp = timestamp
        self.upstream_position_info = upstream_position_info
        self.kafka_position_info = kafka_position_info


class LazyCreateMessage(LazyMessage):

    _message_type = MessageType.create


class LazyDeleteMessage(LazyMessage):

    _message_type = MessageType.delete


class LazyRefreshMessage(LazyMessage):

    _message_type = MessageType.refresh


class LazyUpdateMessage(UpdateMessage):
    """Message for update type. This type of message requires previous
    payload in addition to the payload.

    The payload is lazily schematized using the schema represented by the
    schema id.

    For complete argument docs, see :class:`data_pipeline.message.UpdateMessage`.

    The class derives from :class:`data_pipeline.message.UpdateMessage`
    instead of :class:`data_pipeline.lazy_message.LazyMessage` to keep
    the logic for previous payload, and only provides different constructor
    which takes dictionary payload data and previous payload data instead of
    bytes payload and previous payload.

    Args:
        previous_payload_data (dict): Contents of message.  Message will be lazily
            encoded with schema identified by `schema_id`.  Required when
            message type is MessageType.update.
   """

    def __init__(
        self,
        topic,
        schema_id,
        payload_data,
        previous_payload_data,
        uuid=None,
        contains_pii=False,
        timestamp=None,
        upstream_position_info=None,
        kafka_position_info=None,
        dry_run=False
    ):
        self.dry_run = dry_run

        # payload and previous_payload are lazily constructed only on request
        self._payload = None
        self._previous_payload = None

        self.topic = topic
        self.schema_id = schema_id
        self.payload_data = payload_data
        self.previous_payload_data = previous_payload_data
        self.uuid = uuid
        self.contains_pii = contains_pii
        self.timestamp = timestamp
        self.upstream_position_info = upstream_position_info
        self.kafka_position_info = kafka_position_info
