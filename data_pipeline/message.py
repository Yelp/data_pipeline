# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import namedtuple

from data_pipeline._avro_util import AvroStringReader
from data_pipeline._avro_util import AvroStringWriter
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope
from data_pipeline.message_type import _ProtectedMessageType
from data_pipeline.message_type import MessageType
from data_pipeline.schema_cache import get_schema_cache


logger = get_config().logger

KafkaPositionInfo = namedtuple('KafkaPositionInfo', [
    'offset',               # Offset of the message in the topic
    'partition',            # Partition of the topic the message was from
    'key'                   # Key of the message, may be `None`
])

PayloadFieldDiff = namedtuple('PayloadFieldDiff', [
    'old_value',            # Value of the field before update
    'current_value'         # Value of the field after update
])


class Message(object):
    """Encapsulates a data pipeline message with metadata about the message.

    Validates metadata, but not the payload itself. This class is not meant
    to be used directly. Use specific type message class instead:
    :class:`data_pipeline.message.CreateMessage`,
    :class:`data_pipeline.message.UpdateMessage`,
    :class:`data_pipeline.message.DeleteMessage`, and
    :class:`data_pipeline.message.RefreshMessage`.

    Args:
        topic (str): Kafka topic to publish into
        schema_id (int): Identifies the schema used to encode the payload
        payload (bytes): Avro-encoded message - encoded with schema identified
            by `schema_id`.  Either `payload` or `payload_data` must be provided
            but not both.
        payload_data (dict): The contents of message, which will be lazily
            encoded with schema identified by `schema_id`.  Either `payload` or
            `payload_data` must be provided but not both.
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
        keys (tuple, optional): This should either be a tuple of strings
            or None.  If it's a tuple of strings, the clientlib will combine
            those strings and use them as key when publishing into Kafka.
        dry_run (boolean): When set to True, Message will return a string
            representation of the payload and previous payload, instead of
            the avro encoded message.  This is to avoid loading the schema
            from the schema store.  Defaults to False.

    Remarks:
        Although `previous_payload` and `previous_payload_data` are not
        applicable and do not exist in non-update type Message classes,
        these classes do not prevent them from being added dynamically.
        Ensure not to use these attributes for non-update type Message classes.
    """

    _message_type = None
    """Identifies the nature of the message. The valid value is one of the
    data_pipeline.message_type.MessageType. It must be set by child class.
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
    def message_type(self):
        """Identifies the nature of the message."""
        return self._message_type

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
        self._contains_pii = contains_pii

    @property
    def dry_run(self):
        return self._dry_run

    @dry_run.setter
    def dry_run(self, dry_run):
        self._dry_run = dry_run

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

    @property
    def kafka_position_info(self):
        """The kafka offset, partition, and key of the message if it
        was consumed from kafka. This is expected to be None for messages
        on their way to being published.
        """
        return self._kafka_position_info

    @kafka_position_info.setter
    def kafka_position_info(self, kafka_position_info):
        if kafka_position_info is not None and not isinstance(kafka_position_info, KafkaPositionInfo):
            raise ValueError("kafka_position_info should be None or a KafkaPositionInfo")
        self._kafka_position_info = kafka_position_info

    @property
    def _avro_schema(self):
        return get_schema_cache().get_schema(self.schema_id)

    @property
    def _avro_string_writer(self):
        return AvroStringWriter(
            schema=self._avro_schema
        )

    @property
    def _avro_string_reader(self):
        return AvroStringReader(
            reader_schema=self._avro_schema,
            writer_schema=self._avro_schema
        )

    @property
    def payload(self):
        """Avro-encoded message - encoded with schema identified by `schema_id`.
        """
        self._encode_payload_data_if_necessary()
        return self._payload

    @payload.setter
    def payload(self, payload):
        if not isinstance(payload, bytes):
            raise ValueError("Payload must be bytes")
        self._payload = payload
        self._payload_data = None  # force payload_data to be re-decoded

    @property
    def payload_data(self):
        self._decode_payload_if_necessary()
        return self._payload_data

    @payload_data.setter
    def payload_data(self, payload_data):
        if not isinstance(payload_data, dict):
            raise ValueError("Payload data must be a dict")
        self._payload_data = payload_data
        self._payload = None  # force payload to be re-encoded

    @property
    def keys(self):
        return self._keys

    @keys.setter
    def keys(self, keys):
        if keys is not None and not isinstance(keys, tuple):
            raise ValueError("Keys must be a tuple.")
        if keys and not all(isinstance(key, unicode) for key in keys):
            raise ValueError("Element of keys must be unicode.")
        self._keys = keys

    def __init__(
        self,
        topic,
        schema_id,
        payload=None,
        payload_data=None,
        uuid=None,
        contains_pii=None,
        timestamp=None,
        upstream_position_info=None,
        kafka_position_info=None,
        keys=None,
        dry_run=False
    ):
        # The decision not to just pack the message to validate it is
        # intentional here.  We want to perform more sanity checks than avro
        # does, and in addition, this check is quite a bit faster than
        # serialization.  Finally, if we do it this way, we can lazily
        # serialize the payload in a subclass if necessary.
        self.topic = topic
        self.schema_id = schema_id
        self.uuid = uuid
        self.contains_pii = contains_pii
        self.timestamp = timestamp
        self.upstream_position_info = upstream_position_info
        self.kafka_position_info = kafka_position_info
        self.keys = keys
        self.dry_run = dry_run
        self._set_payload_or_payload_data(payload, payload_data)

    def _set_payload_or_payload_data(self, payload, payload_data):
        # payload or payload_data are lazily constructed only on request
        is_not_none_payload = payload is not None
        is_not_none_payload_data = payload_data is not None

        if is_not_none_payload and is_not_none_payload_data:
            raise ValueError("Cannot pass both payload and payload_data.")
        if is_not_none_payload:
            self.payload = payload
        elif is_not_none_payload_data:
            self.payload_data = payload_data
        else:
            raise ValueError("Either payload or payload_data must be provided.")

    def __eq__(self, other):
        return self.__key == other.__key

    def __ne__(self, other):
        return self.__key != other.__key

    def __hash__(self):
        return hash(self.__key)

    @property
    def __key(self):
        """Returns a tuple representing a unique key for this Message.

        Note:
            We don't include `payload_data` in the key tuple as we should be
            confident that if `payload` matches then `payload_data` will as
            well, and there is an extra overhead from decoding.
        """
        return (
            self.__class__,
            self.message_type,
            self.topic,
            self.schema_id,
            self.payload,
            self.uuid,
            self.contains_pii,
            self.timestamp,
            self.upstream_position_info,
            self.kafka_position_info,
            self.dry_run
        )

    @property
    def avro_repr(self):
        return {
            'uuid': self.uuid,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload': self.payload,
            'timestamp': self.timestamp
        }

    def _encode_payload_data_if_necessary(self):
        if self._payload is None:
            self._payload = self._encode_data(self._payload_data)

    def _encode_data(self, data):
        """Encodes data, returning a repr in dry_run mode"""
        if self.dry_run:
            return repr(data)
        return self._avro_string_writer.encode(message_avro_representation=data)

    def _decode_payload_if_necessary(self):
        if self._payload_data is None:
            self._payload_data = self._avro_string_reader.decode(
                encoded_message=self._payload
            )

    def reload_data(self):
        """Encode the payload data or decode the payload if it hasn't done so.
        """
        pii_flag = get_schema_cache().get_contains_pii_for_schema_id(self.schema_id)
        self.contains_pii = pii_flag
        self._decode_payload_if_necessary()
        self._encode_payload_data_if_necessary()


class CreateMessage(Message):

    _message_type = MessageType.create


class DeleteMessage(Message):

    _message_type = MessageType.delete


class RefreshMessage(Message):

    _message_type = MessageType.refresh


class LogMessage(Message):
    _message_type = MessageType.log


class MonitorMessage(Message):
    _message_type = _ProtectedMessageType.monitor


class UpdateMessage(Message):
    """Message for update type. This type of message requires previous
    payload in addition to the payload.

    For complete argument docs, see :class:`data_pipeline.message.Message`.

    Args:
        previous_payload (bytes): Avro-encoded message - encoded with schema
            identified by `schema_id`  Required when message type is
            MessageType.update.  Either `previous_payload` or `previous_payload_data`
            must be provided but not both.
        previous_payload_data (dict): The contents of message, which will be
            lazily encoded with schema identified by `schema_id`.  Required
            when message type is MessageType.update.  Either `previous_payload`
            or `previous_payload_data` must be provided but not both.
    """

    _message_type = MessageType.update

    def __init__(
        self,
        topic,
        schema_id,
        payload=None,
        payload_data=None,
        previous_payload=None,
        previous_payload_data=None,
        uuid=None,
        contains_pii=False,
        timestamp=None,
        upstream_position_info=None,
        kafka_position_info=None,
        keys=None,
        dry_run=False
    ):
        super(UpdateMessage, self).__init__(
            topic,
            schema_id,
            payload=payload,
            payload_data=payload_data,
            uuid=uuid,
            contains_pii=contains_pii,
            timestamp=timestamp,
            upstream_position_info=upstream_position_info,
            kafka_position_info=kafka_position_info,
            keys=keys,
            dry_run=dry_run
        )
        self._set_previous_payload_or_payload_data(
            previous_payload,
            previous_payload_data
        )

    def _set_previous_payload_or_payload_data(
        self,
        previous_payload,
        previous_payload_data
    ):
        # previous_payload or previous_payload_data are lazily constructed
        # only on request
        is_not_none_previous_payload = previous_payload is not None
        is_not_none_previous_payload_data = previous_payload_data is not None

        if is_not_none_previous_payload and is_not_none_previous_payload_data:
            raise ValueError(
                "Cannot pass both previous_payload and previous_payload_data."
            )
        if is_not_none_previous_payload:
            self.previous_payload = previous_payload
        elif is_not_none_previous_payload_data:
            self.previous_payload_data = previous_payload_data
        else:
            raise ValueError(
                "Either previous_payload or previous_payload_data must be provided."
            )

    @property
    def __key(self):
        """Returns a tuple representing a unique key for this Message.

        Note:
            We don't include `previous_payload_data` in the key as we should
            be confident that if `previous_payload` matches then
            `previous_payload_data` will as well, and there is an extra
            overhead from decoding.
        """
        return super(UpdateMessage, self).__key + (self.previous_payload, )

    @property
    def previous_payload(self):
        """Avro-encoded message - encoded with schema identified by
        `schema_id`.  Required when message type is `MessageType.update`.
        """
        self._encode_previous_payload_data_if_necessary()
        return self._previous_payload

    @previous_payload.setter
    def previous_payload(self, previous_payload):
        if not isinstance(previous_payload, bytes):
            raise ValueError("Previous payload must be bytes")
        self._previous_payload = previous_payload
        self._previous_payload_data = None  # force previous_payload_data to be re-decoded

    @property
    def previous_payload_data(self):
        self._decode_previous_payload_if_necessary()
        return self._previous_payload_data

    @previous_payload_data.setter
    def previous_payload_data(self, previous_payload_data):
        if not isinstance(previous_payload_data, dict):
            raise ValueError("Previous payload data must be a dict")

        self._previous_payload_data = previous_payload_data
        self._previous_payload = None  # force previous_payload to be re-encoded

    @property
    def avro_repr(self):
        return {
            'uuid': self.uuid,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload': self.payload,
            'previous_payload': self.previous_payload,
            'timestamp': self.timestamp
        }

    def _encode_previous_payload_data_if_necessary(self):
        if self._previous_payload is None:
            self._previous_payload = self._encode_data(self._previous_payload_data)

    def _decode_previous_payload_if_necessary(self):
        if self._previous_payload_data is None:
            self._previous_payload_data = self._avro_string_reader.decode(
                encoded_message=self._previous_payload
            )

    def reload_data(self):
        """Encode the previous payload data or decode the previous payload
        if it hasn't done so. The payload encoding/payload data decoding is
        taken care of by the `Message.reload` function in the parent class.
        """
        super(UpdateMessage, self).reload_data()
        self._decode_previous_payload_if_necessary()
        self._encode_previous_payload_data_if_necessary()

    def _has_field_changed(self, field):
        return self.payload_data[field] != self.previous_payload_data[field]

    def _get_field_diff(self, field):
        return PayloadFieldDiff(
            old_value=self.previous_payload_data[field],
            current_value=self.payload_data[field]
        )

    @property
    def has_changed(self):
        return any(
            self._has_field_changed(field)
            for field in self.payload_data.iterkeys()
        )

    @property
    def payload_diff(self):
        return {
            field: self._get_field_diff(field)
            for field in self.payload_data.iterkeys()
            if self._has_field_changed(field)
        }

_message_type_to_class_map = {
    o._message_type.name: o for o in Message.__subclasses__() if o._message_type
}


def create_from_kafka_message(
        topic,
        kafka_message,
        force_payload_decoding=True
):
    """ Build a data_pipeline.message.Message from a yelp_kafka message

    Args:
        topic (str): The topic name from which the message was received.
        kafka_message (yelp_kafka.consumer.Message): The message info which
            has the payload, offset, partition, and key of the received
            message.
        force_payload_decoding (boolean): If this is set to `True` then
            we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.

    Returns (class:`data_pipeline.message.Message`):
        The message object
    """
    kafka_position_info = KafkaPositionInfo(
        offset=kafka_message.offset,
        partition=kafka_message.partition,
        key=kafka_message.key,
    )
    return _create_message_from_packed_message(
        topic=topic,
        packed_message=kafka_message,
        force_payload_decoding=force_payload_decoding,
        kafka_position_info=kafka_position_info
    )


def create_from_offset_and_message(
        topic,
        offset_and_message,
        force_payload_decoding=True
):
    """ Build a data_pipeline.message.Message from a kafka.common.OffsetAndMessage

    Args:
        topic (str): The topic name from which the message was received.
        offset_and_message (kafka.common.OffsetAndMessage): a namedtuple
            containing the offset and message. Message contains magic,
            attributes, keys and values.
        force_payload_decoding (boolean): If this is set to `True` then
            we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.

    Returns (data_pipeline.message.Message):
        The message object
    """
    return _create_message_from_packed_message(
        topic=topic,
        packed_message=offset_and_message.message,
        force_payload_decoding=force_payload_decoding
    )


def _create_message_from_packed_message(
    topic,
    packed_message,
    force_payload_decoding,
    kafka_position_info=None
):
    """ Builds a data_pipeline.message.Message from packed_message
    Args:
        topic (str): The topic name from which the message was received.
        packed_message (yelp_kafka.consumer.Message or kafka.common.Message):
            The message info which has the payload, offset, partition,
            and key of the received message if of type yelp_kafka.consumer.message
            or just payload, uuid, schema_id in case of kafka.common.Message.
        force_payload_decoding (boolean): If this is set to `True` then
            we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.
        append_kafka_position_info (boolean): If this is set to `True` then
            we will construct kafka_position_info for resulting message
            from the unpacked_message. Otherwise kafka_position_info will
            be set to None.

    Returns (data_pipeline.message.Message):
        The message object
    """
    unpacked_message = Envelope().unpack(packed_message.value)
    message_class = _message_type_to_class_map[unpacked_message['message_type']]
    message_params = {
        'topic': topic,
        'uuid': unpacked_message['uuid'],
        'schema_id': unpacked_message['schema_id'],
        'payload': unpacked_message['payload'],
        'timestamp': unpacked_message['timestamp'],
        'kafka_position_info': kafka_position_info
    }
    if message_class is UpdateMessage:
        message_params.update(
            {'previous_payload': unpacked_message['previous_payload']}
        )
    message = message_class(**message_params)
    if force_payload_decoding:
        # Access the cached, but lazily-calculated, properties
        message.reload_data()
    return message
