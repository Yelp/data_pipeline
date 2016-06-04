# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import namedtuple
from uuid import UUID

from yelp_lib.containers.lists import unlist

from data_pipeline._encryption_helper import EncryptionHelper
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.message_type import _ProtectedMessageType
from data_pipeline.message_type import MessageType
from data_pipeline.meta_attribute import MetaAttribute
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


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


class NoEntryPayload(object):
    """ This class denotes that no previous value exists for the field. """
    pass


class InvalidOperation(Exception):
    pass


class Message(object):
    """Encapsulates a data pipeline message with metadata about the message.

    Validates metadata, but not the payload itself. This class is not meant
    to be used directly. Use specific type message class instead:
    :class:`data_pipeline.message.CreateMessage`,
    :class:`data_pipeline.message.UpdateMessage`,
    :class:`data_pipeline.message.DeleteMessage`, and
    :class:`data_pipeline.message.RefreshMessage`.

    Args:
        schema_id (int): Identifies the schema used to encode the payload.
        reader_schema_id (Optional[int]): Identifies the schema used to decode
            the payload.
        topic (Optional[str]): Kafka topic to publish into.  It is highly
            recommended to leave it unassigned and let the Schematizer decide
            the topic of the schema.  Use caution when overriding the topic.
        payload (bytes): Avro-encoded message - encoded with schema identified
            by `schema_id`. This is expected to be None for messages on their
            way to being published. Either `payload` or `payload_data` must be
            provided but not both.
        payload_data (dict): The contents of message, which will be lazily
            encoded with schema identified by `schema_id`.  Either `payload` or
            `payload_data` must be provided but not both.
        uuid (bytes, optional): Globally-unique 16-byte identifier for the
            message.  A uuid4 will be generated automatically if this isn't
            provided.
        contains_pii (bool, optional): Indicates that the payload contains PII,
            so the clientlib can properly encrypt the data and mark it as
            sensitive, defaults to False. The data pipeline consumer will
            automatically decrypt fields containing PII. This field should not
            be used to indicate that a topic should be encrypted, because
            PII information will be used to indicate to various systems how
            to handle the data, in addition to automatic decryption.
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
        meta (list of MetaAttribute, optional): This should be a list of
            MetaAttribute objects or None. This is used to contain information
            about metadata. These meta attributes are serialized using their
            respective avro schema, which is registered with the schematizer.
            Hence meta should be set with a dict which contains schema_id and
            payload as keys to construct the MetaAttribute objects. The
            payload is deserialized using the schema_id.

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
    def _schematizer(self):
        return get_schematizer()

    @property
    def topic(self):
        return self._topic

    def _set_topic(self, topic):
        if not isinstance(topic, str):
            raise TypeError("Topic must be a non-empty string")
        if len(topic) == 0:
            raise ValueError("Topic must be a non-empty string")
        self._topic = topic

    @property
    def schema_id(self):
        return self._schema_id

    def _set_schema_id(self, schema_id):
        if not isinstance(schema_id, int):
            raise TypeError("Schema id should be an int")
        self._schema_id = schema_id

    @property
    def reader_schema_id(self):
        return self._reader_schema_id

    def _set_reader_schema_id(self, reader_schema_id):
        if not self._is_valid_optional_type(reader_schema_id, int):
            raise TypeError("Reader schema id must be an int")
        self._reader_schema_id = reader_schema_id or self.schema_id

    @property
    def message_type(self):
        """Identifies the nature of the message."""
        return self._message_type

    @property
    def uuid(self):
        return self._uuid

    def _set_uuid(self, uuid):
        if uuid is None:
            # UUID generation is expensive.  Using FastUUID instead of the built
            # in UUID methods increases Messages that can be instantiated per
            # second from ~25,000 to ~185,000.  Not generating UUIDs at all
            # increases the throughput further still to about 730,000 per
            # second.
            uuid = self._fast_uuid.uuid4()
        elif len(uuid) != 16:
            raise TypeError(
                "UUIDs should be exactly 16 bytes.  Conforming UUID's can be "
                "generated with `import uuid; uuid.uuid4().bytes`."
            )
        self._uuid = uuid

    @property
    def uuid_hex(self):
        # TODO: DATAPIPE-848
        return UUID(bytes=self.uuid).hex

    @property
    def contains_pii(self):
        return self._contains_pii

    def _set_contains_pii(self, schema_id):
        self._contains_pii = self._schematizer.get_schema_by_id(
            schema_id
        ).topic.contains_pii

    @property
    def encryption_type(self):
        return self._encryption_type

    def _set_encryption_type(self):
        self._encryption_type = None
        if not self._should_be_encrypted:
            return

        config_encryption_type = get_config().encryption_type
        if config_encryption_type is None:
            raise ValueError(
                "Encryption type must be set when message requires to be encrypted."
            )
        self._manual_set_encryption_type(config_encryption_type)

    @property
    def _should_be_encrypted(self):
        """Whether this message should be encrypted.  So far the criteria used
        to determine if the message should be encrypted is the pii information.
        Include additional criteria if necessary.
        """
        return self.contains_pii

    def _manual_set_encryption_type(self, encryption_type):
        """Manually set the encryption type of the message.  It is for the case
        when not all the criteria used to determine if this message should be
        encryption is available, such as when constructing the message from a
        published kafka message.
        """
        self._encryption_type = encryption_type
        if encryption_type:
            self._encryption_helper = EncryptionHelper(encryption_type)

    @property
    def dry_run(self):
        return self._dry_run

    def _set_dry_run(self, dry_run):
        self._dry_run = dry_run

    @property
    def meta(self):
        return self._meta

    def _set_meta(self, meta):
        if (not self._is_valid_optional_type(meta, list) or
                self._any_invalid_type(meta, MetaAttribute)):
            raise TypeError("Meta must be None or list of MetaAttribute objects.")
        self._meta = meta

    def get_meta_attr_by_type(self, meta, meta_type):
        if meta is not None:
            attributes_with_type = [m for m in meta if m.source == meta_type]
            return unlist(attributes_with_type)
        return None

    def _get_meta_attr_avro_repr(self):
        if self.meta is not None:
            return [meta_attr.avro_repr for meta_attr in self.meta]
        return None

    @property
    def timestamp(self):
        return self._timestamp

    def _set_timestamp(self, timestamp):
        if timestamp is None:
            timestamp = int(time.time())
        self._timestamp = timestamp

    @property
    def upstream_position_info(self):
        return self._upstream_position_info

    def _set_upstream_position_info(self, upstream_position_info):
        if not self._is_valid_optional_type(upstream_position_info, dict):
            raise TypeError("upstream_position_info must be None or a dict")
        self._upstream_position_info = upstream_position_info

    @upstream_position_info.setter
    def upstream_position_info(self, upstream_position_info):
        # This should be the only exception that users can update the data after
        # the message is created. The `upstream_position_info` is not used in
        # the data pipeline and the data is up to the application, so it should
        # be ok. It is more efficient and simpler to allow application updating
        # the data than creating new instance with new data each time.
        self._set_upstream_position_info(upstream_position_info)

    @property
    def kafka_position_info(self):
        """The kafka offset, partition, and key of the message if it
        was consumed from kafka. This is expected to be None for messages
        on their way to being published.
        """
        return self._kafka_position_info

    def _set_kafka_position_info(self, kafka_position_info):
        if not self._is_valid_optional_type(kafka_position_info, KafkaPositionInfo):
            raise TypeError(
                "kafka_position_info must be None or a KafkaPositionInfo"
            )
        self._kafka_position_info = kafka_position_info

    @property
    def _avro_schema(self):
        return self._schematizer.get_schema_by_id(self.schema_id).schema_json

    @property
    def _avro_reader_schema(self):
        return self._schematizer.get_schema_by_id(self.reader_schema_id).schema_json

    @property
    def _avro_string_writer(self):
        """get the writer from store if already exists"""
        return _AvroStringStore().get_writer(self.schema_id)

    @property
    def _avro_string_reader(self):
        """get the reader from store if already exists"""
        return _AvroStringStore().get_reader(
            reader_schema_id=self.reader_schema_id,
            writer_schema_id=self.schema_id
        )

    @property
    def payload(self):
        self._set_payload_if_necessary(self._payload_data)
        return self._payload

    def _set_payload(self, payload):
        if not isinstance(payload, bytes):
            raise TypeError("Payload must be bytes")
        self._payload = payload
        self._payload_data = None  # force payload_data to be re-decoded

    @property
    def payload_data(self):
        self._set_payload_data_if_necessary(self._payload)
        return self._payload_data

    def _set_payload_data(self, payload_data):
        if not isinstance(payload_data, dict):
            raise TypeError("Payload data must be a dict")
        self._payload_data = payload_data
        self._payload = None  # force payload to be re-encoded

    @property
    def keys(self):
        return self._keys

    def _set_keys(self, keys):
        if not self._is_valid_optional_type(keys, tuple):
            raise TypeError("Keys must be None or a tuple.")
        if self._any_invalid_type(keys, unicode):
            raise TypeError("Element of keys must be unicode.")
        self._keys = keys

    @property
    def payload_diff(self):
        return {
            field: self._get_field_diff(field) for field in self.payload_data
        }

    def __init__(
        self,
        schema_id,
        reader_schema_id=None,
        topic=None,
        payload=None,
        payload_data=None,
        uuid=None,
        contains_pii=None,
        timestamp=None,
        upstream_position_info=None,
        kafka_position_info=None,
        keys=None,
        dry_run=False,
        meta=None
    ):
        # The decision not to just pack the message, but to validate it, is
        # intentional here.  We want to perform more sanity checks than avro
        # does, and in addition, this check is quite a bit faster than
        # serialization.  Finally, if we do it this way, we can lazily
        # serialize the payload in a subclass if necessary.
        self._set_schema_id(schema_id)
        self._set_reader_schema_id(reader_schema_id)
        self._set_topic(
            topic or str(self._schematizer.get_schema_by_id(schema_id).topic.name)
        )
        self._set_uuid(uuid)
        self._set_timestamp(timestamp)
        self._set_upstream_position_info(upstream_position_info)
        self._set_kafka_position_info(kafka_position_info)
        self._set_keys(keys)
        self._set_dry_run(dry_run)
        # TODO(DATAPIPE-416|psuben): Make it so contains_pii is no longer overrideable.
        self._set_contains_pii(schema_id)
        self._set_encryption_type()
        self._set_meta(meta)
        self._set_payload_or_payload_data(payload, payload_data)
        if topic:
            logger.debug(
                "Overriding message topic: {} for schema {}.".format(topic, schema_id)
            )
        if self.encryption_type:
            if self._meta is None:
                self._meta = []
            if not self.get_encryption_meta(self.encryption_type, self.meta):
                self.meta.append(self._encryption_helper.encryption_meta)

    def _set_payload_or_payload_data(self, payload, payload_data):
        # payload or payload_data are lazily constructed only on request
        is_not_none_payload = payload is not None
        is_not_none_payload_data = payload_data is not None

        if is_not_none_payload and is_not_none_payload_data:
            raise TypeError("Cannot pass both payload and payload_data.")
        if is_not_none_payload:
            self._set_payload(payload)
        elif is_not_none_payload_data:
            self._set_payload_data(payload_data)
        else:
            raise TypeError("Either payload or payload_data must be provided.")

    def _is_valid_optional_type(self, value, typ):
        return value is None or isinstance(value, typ)

    def _any_invalid_type(self, value_list, typ):
        if not value_list:
            return False
        return any(not isinstance(value, typ) for value in value_list)

    def _encode_payload_data(self, payload_data):
        if self.dry_run:
            return repr(payload_data)
        return self._avro_string_writer.encode(
            message_avro_representation=payload_data
        )

    def _decode_payload(self, payload):
        return self._avro_string_reader.decode(
            encoded_message=payload
        )

    def _encrypt_payload_if_necessary(self, payload):
        if self.encryption_type is not None:
            return self._encryption_helper.encrypt_payload(payload)
        return payload

    def _set_payload_data_if_necessary(self, payload):
        if self._payload_data is None:
            self._payload_data = self._decode_payload(payload)

    def _set_payload_if_necessary(self, payload_data):
        if self._payload is None:
            self._payload = self._encode_payload_data(payload_data)

    @property
    def avro_repr(self):
        return {
            'uuid': self.uuid,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload': self._encrypt_payload_if_necessary(self.payload),
            'timestamp': self.timestamp,
            'meta': self._get_meta_attr_avro_repr(),
            'encryption_type': self.encryption_type,
        }

    @classmethod
    def create_from_unpacked_message(
        cls,
        unpacked_message,
        topic=None,
        kafka_position_info=None,
        reader_schema_id=None
    ):
        encryption_type = unpacked_message['encryption_type']
        meta = cls._get_unpacked_meta(unpacked_message)
        payload = cls._get_unpacked_decrypted_payload(
            unpacked_message['payload'],
            encryption_type=encryption_type,
            meta=meta
        )

        message_params = {
            'uuid': unpacked_message['uuid'],
            'schema_id': unpacked_message['schema_id'],
            'reader_schema_id': reader_schema_id,
            'payload': payload,
            'timestamp': unpacked_message['timestamp'],
            'meta': meta,
            'kafka_position_info': kafka_position_info
        }
        if topic:
            message_params['topic'] = topic
        message = cls(**message_params)
        message._manual_set_encryption_type(encryption_type)
        return message

    @classmethod
    def _get_unpacked_meta(cls, unpacked_message):
        return [
            MetaAttribute(schema_id=o['schema_id'], encoded_payload=o['payload'])
            for o in unpacked_message['meta']
        ] if unpacked_message['meta'] else None

    @classmethod
    def _get_unpacked_decrypted_payload(cls, payload, encryption_type, meta):
        if not encryption_type:
            return payload

        encryption_meta = cls.get_encryption_meta(encryption_type, meta)
        encryption_helper = EncryptionHelper(encryption_type, encryption_meta)
        return encryption_helper.decrypt_payload(payload)

    @classmethod
    def get_encryption_meta(cls, encryption_type, meta):
        encryption_meta = EncryptionHelper.get_encryption_meta_by_encryption_type(
            encryption_type
        )
        return next(
            (m for m in meta if m.schema_id == encryption_meta.schema_id),
            None
        )

    def reload_data(self):
        """Populate the payload data or the payload if it hasn't done so.
        """
        self._set_payload_data_if_necessary(self._payload)
        self._set_payload_if_necessary(self._payload_data)

    def _cleaned_pii_data_copy(self, data):
        if not isinstance(data, dict):
            return unicode(type(data))
        return {key: self._cleaned_pii_data_copy(value) for key, value in data.iteritems()}

    @property
    def _str_repr(self):
        cleaned_payload_data = self.payload_data
        if self.contains_pii:
            cleaned_payload_data = self._cleaned_pii_data_copy(self.payload_data)
        return {
            'uuid': self.uuid_hex,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload_data': cleaned_payload_data,
            'timestamp': self.timestamp,
            'meta': self._get_meta_attr_avro_repr(),
            'encryption_type': self.encryption_type
        }

    def __str__(self):
        return str(self._str_repr)

    def __eq__(self, other):
        return type(self) is type(other) and self._eq_key == other._eq_key

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._eq_key)

    @property
    def _eq_key(self):
        """Returns a tuple representing a unique key for this Message.

        Note:
            We don't include `payload_data` in the key tuple as we should be
            confident that if `payload` matches then `payload_data` will as
            well, and there is an extra overhead from decoding.
        """
        return (
            self.message_type,
            self.topic,
            self.schema_id,
            self.payload,
            self.uuid,
            self.timestamp,
            self.upstream_position_info,
            self.kafka_position_info,
            self.dry_run,
            self.encryption_type
        )


class CreateMessage(Message):

    _message_type = MessageType.create

    def _get_field_diff(self, field):
        return PayloadFieldDiff(
            old_value=NoEntryPayload,
            current_value=self.payload_data[field]
        )


class DeleteMessage(Message):

    _message_type = MessageType.delete

    def _get_field_diff(self, field):
        return PayloadFieldDiff(
            old_value=self.payload_data[field],
            current_value=NoEntryPayload
        )


class RefreshMessage(Message):

    _message_type = MessageType.refresh

    def _get_field_diff(self, field):
        raise InvalidOperation()


class LogMessage(Message):
    _message_type = MessageType.log

    def _get_field_diff(self, field):
        raise InvalidOperation()


class MonitorMessage(Message):
    _message_type = _ProtectedMessageType.monitor

    def _get_field_diff(self, field):
        raise InvalidOperation()


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
        schema_id,
        reader_schema_id=None,
        topic=None,
        payload=None,
        payload_data=None,
        previous_payload=None,
        previous_payload_data=None,
        uuid=None,
        contains_pii=None,
        timestamp=None,
        upstream_position_info=None,
        kafka_position_info=None,
        keys=None,
        dry_run=False,
        meta=None,
    ):
        super(UpdateMessage, self).__init__(
            schema_id,
            reader_schema_id,
            topic=topic,
            payload=payload,
            payload_data=payload_data,
            uuid=uuid,
            contains_pii=contains_pii,
            timestamp=timestamp,
            upstream_position_info=upstream_position_info,
            kafka_position_info=kafka_position_info,
            keys=keys,
            dry_run=dry_run,
            meta=meta,
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
            raise TypeError(
                "Cannot pass both previous_payload and previous_payload_data."
            )
        if is_not_none_previous_payload:
            self._set_previous_payload(previous_payload)
        elif is_not_none_previous_payload_data:
            self._set_previous_payload_data(previous_payload_data)
        else:
            raise TypeError(
                "Either previous_payload or previous_payload_data must be provided."
            )

    @property
    def _eq_key(self):
        """Returns a tuple representing a unique key for this Message.

        Note:
            We don't include `previous_payload_data` in the key as we should
            be confident that if `previous_payload` matches then
            `previous_payload_data` will as well, and there is an extra
            overhead from decoding.
        """
        return super(UpdateMessage, self)._eq_key + (self.previous_payload,)

    @property
    def previous_payload(self):
        """Avro-encoded message - encoded with schema identified by
        `schema_id`.  Required when message type is `MessageType.update`.
        """
        self._set_previous_payload_if_necessary(self._previous_payload_data)
        return self._previous_payload

    def _set_previous_payload(self, previous_payload):
        if not isinstance(previous_payload, bytes):
            raise TypeError("Previous payload must be bytes")
        self._previous_payload = previous_payload
        self._previous_payload_data = None  # force previous_payload_data to be re-decoded

    @property
    def previous_payload_data(self):
        self._set_previous_payload_data_if_necessary(self._previous_payload)
        return self._previous_payload_data

    def _set_previous_payload_data(self, previous_payload_data):
        if not isinstance(previous_payload_data, dict):
            raise TypeError("Previous payload data must be a dict")

        self._previous_payload_data = previous_payload_data
        self._previous_payload = None  # force previous_payload to be re-encoded

    @property
    def avro_repr(self):
        repr_dict = super(UpdateMessage, self).avro_repr
        repr_dict['previous_payload'] = self._encrypt_payload_if_necessary(
            self.previous_payload
        )
        return repr_dict

    @classmethod
    def create_from_unpacked_message(
        cls,
        unpacked_message,
        topic=None,
        kafka_position_info=None,
        reader_schema_id=None
    ):
        encryption_type = unpacked_message['encryption_type']
        meta = cls._get_unpacked_meta(unpacked_message)
        payload = cls._get_unpacked_decrypted_payload(
            unpacked_message['payload'],
            encryption_type=encryption_type,
            meta=meta
        )
        previous_payload = cls._get_unpacked_decrypted_payload(
            unpacked_message['previous_payload'],
            encryption_type=encryption_type,
            meta=meta
        )

        message_params = {
            'uuid': unpacked_message['uuid'],
            'schema_id': unpacked_message['schema_id'],
            'reader_schema_id': reader_schema_id,
            'payload': payload,
            'previous_payload': previous_payload,
            'timestamp': unpacked_message['timestamp'],
            'meta': meta,
            'kafka_position_info': kafka_position_info
        }
        if topic:
            message_params['topic'] = topic
        message = cls(**message_params)
        message._manual_set_encryption_type(encryption_type)
        return message

    def _set_previous_payload_if_necessary(self, previous_payload_data):
        if self._previous_payload is None:
            self._previous_payload = self._encode_payload_data(
                previous_payload_data
            )

    def _set_previous_payload_data_if_necessary(self, previous_payload):
        if self._previous_payload_data is None:
            self._previous_payload_data = self._decode_payload(previous_payload)

    def reload_data(self):
        """Populate the previous payload data or decode the previous payload
        if it hasn't done so. The payload encoding/payload data decoding is
        taken care of by the `Message.reload` function in the parent class.
        """
        super(UpdateMessage, self).reload_data()
        self._set_previous_payload_data_if_necessary(self._previous_payload)
        self._set_previous_payload_if_necessary(self._previous_payload_data)

    def _has_field_changed(self, field):
        return self.payload_data[field] != self.previous_payload_data[field]

    def _get_field_diff(self, field):
        return PayloadFieldDiff(
            old_value=self.previous_payload_data[field],
            current_value=self.payload_data[field]
        )

    @property
    def has_changed(self):
        return any(self._has_field_changed(field) for field in self.payload_data)

    @property
    def payload_diff(self):
        return {
            field: self._get_field_diff(field)
            for field in self.payload_data if self._has_field_changed(field)
        }

    @property
    def _str_repr(self):
        # Calls the _str_repr from the super class resulting in encrypted pii data.
        repr_dict = super(UpdateMessage, self)._str_repr
        repr_dict['previous_payload_data'] = (
            self.previous_payload_data if not self.contains_pii
            else self._cleaned_pii_data_copy(self.previous_payload_data)
        )

        return repr_dict


_message_type_to_class_map = {
    o._message_type.name: o for o in Message.__subclasses__() if o._message_type
}


def create_from_kafka_message(
    topic,
    kafka_message,
    force_payload_decoding=True,
    reader_schema_id=None
):
    """ Build a data_pipeline.message.Message from a yelp_kafka message. If no
    reader schema id is provided, the schema used for encoding will be used for
    decoding.

    Args:
        topic (str): The topic name from which the message was received.
            This parameter is deprecating and currently not used. The topic
            will be retrieved from the given `kafka_message`.
        kafka_message (kafka.common.KafkaMessage): The message info which
            has the topic, partition, offset, key, and value(payload) of
            the received message.
        force_payload_decoding (boolean): If this is set to `True` then
            we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.
        reader_schema_id (Optional[int]): Schema id used to decode the
            kafka_message and build data_pipeline.message.Message message.
            Defaults to None.


    Returns (class:`data_pipeline.message.Message`):
        The message object
    """
    kafka_position_info = KafkaPositionInfo(
        offset=kafka_message.offset,
        partition=kafka_message.partition,
        key=kafka_message.key,
    )
    return _create_message_from_packed_message(
        topic=kafka_message.topic,
        packed_message=kafka_message,
        force_payload_decoding=force_payload_decoding,
        kafka_position_info=kafka_position_info,
        reader_schema_id=reader_schema_id
    )


def create_from_offset_and_message(
    topic,
    offset_and_message,
    force_payload_decoding=True,
    reader_schema_id=None
):
    """
    Build a data_pipeline.message.Message from a kafka.common.OffsetAndMessage.
    If no reader schema id is provided, the schema used for encoding will be
    used for decoding.

    Args:
        topic (str): The topic name from which the message was received.
        offset_and_message (kafka.common.OffsetAndMessage): a namedtuple
            containing the offset and message. Message contains magic,
            attributes, keys and values.
        force_payload_decoding (boolean): If this is set to `True` then
            we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.
        reader_schema_id (Optional[int]): Schema id used to decode the incoming
            kafka message and build data_pipeline.message.Message message.
            Defaults to None.

    Returns (data_pipeline.message.Message):
        The message object
    """
    return _create_message_from_packed_message(
        topic=topic,
        packed_message=offset_and_message.message,
        force_payload_decoding=force_payload_decoding,
        reader_schema_id=reader_schema_id
    )


def _create_message_from_packed_message(
    topic,
    packed_message,
    force_payload_decoding,
    kafka_position_info=None,
    reader_schema_id=None
):
    """ Builds a data_pipeline.message.Message from packed_message. If no
    reader schema id is provided, the schema used for encoding will be used for
    decoding.

    Args:
        topic (str): the topic name where the message comes from.
        packed_message (yelp_kafka.consumer.Message or kafka.common.KafkaMessage):
            The message info which has the payload, offset, partition,
            and key of the received message if of type yelp_kafka.consumer.message
            or just payload, uuid, schema_id in case of kafka.common.Message.
        force_payload_decoding (boolean): If this is set to `True` then
            we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.
        kafka_position_info (Optional[KafkaPositionInfo]): The specified kafka
            position information.  The kafka_position_info may be constructed
            from the unpacked yelp_kafka message.
        reader_schema_id (Optional[int]): Schema id used to decode the incoming
            kafka message and build data_pipeline.message.Message message.
            Defaults to None.

    Returns (data_pipeline.message.Message):
        The message object
    """
    unpacked_message = Envelope().unpack(packed_message.value)
    message_class = _message_type_to_class_map[unpacked_message['message_type']]
    message = message_class.create_from_unpacked_message(
        unpacked_message=unpacked_message,
        topic=topic,
        kafka_position_info=kafka_position_info,
        reader_schema_id=reader_schema_id
    )
    if force_payload_decoding:
        # Access the cached, but lazily-calculated, properties
        message.reload_data()
    return message
