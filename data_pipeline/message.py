# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import time
import warnings
from collections import namedtuple
from uuid import UUID

from data_pipeline._avro_payload import _AvroPayload
from data_pipeline._encryption_helper import EncryptionHelper
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope
from data_pipeline.helpers.lists import unlist
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


class MissingMetaAttributeException(Exception):
    def __init__(self, schema_id, meta_ids, mandatory_meta_ids):
        Exception.__init__(
            self,
            "Meta Attributes with IDs `{0}` are not found for schema_id "
            "`{1}`.".format(
                ", ".join(str(m) for m in (mandatory_meta_ids - meta_ids)),
                schema_id
            ))


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
        payload_data: The contents of message, which will be lazily
            encoded with schema identified by `schema_id`. Either `payload` or
            `payload_data` must be provided but not both. Type of payload_data
            should match the avro type specified schema.
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
        return self._avro_payload.schema_id

    @property
    def reader_schema_id(self):
        return self._avro_payload.reader_schema_id

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
        if self._contains_pii is not None:
            return self._contains_pii
        self._set_contains_pii()
        return self._contains_pii

    def _set_contains_pii(self):
        self._contains_pii = self._schematizer.get_schema_by_id(
            self.schema_id
        ).topic.contains_pii

    @property
    def encryption_type(self):
        self._set_encryption_type_if_necessary()
        return self._encryption_type

    def _set_encryption_type_if_necessary(self):
        if self._encryption_type or not self._should_be_encrypted:
            return
        config_encryption_type = get_config().encryption_type
        if config_encryption_type is None:
            raise ValueError(
                "Encryption type must be set when message requires to be encrypted."
            )
        self._encryption_type = config_encryption_type
        self._encryption_helper = EncryptionHelper(config_encryption_type)
        self._set_encryption_meta()

    @property
    def _should_be_encrypted(self):
        """Whether this message should be encrypted.  So far the criteria used
        to determine if the message should be encrypted is the pii information.
        Include additional criteria if necessary.
        """
        if self._should_be_encrypted_state is not None:
            return self._should_be_encrypted_state

        self._should_be_encrypted_state = self.contains_pii
        return self._should_be_encrypted_state

    def _set_encryption_meta(self):
        if self._meta is None:
            self._meta = []
        self._pop_encryption_meta(self._encryption_type, self._meta)
        self._meta.append(self._encryption_helper.encryption_meta)

    @property
    def dry_run(self):
        return self._avro_payload.dry_run

    @property
    def meta(self):
        self._set_encryption_type_if_necessary()
        return self._meta

    def _set_meta(self, meta, schema_id):
        if (not self._is_valid_optional_type(meta, list) or
                self._any_invalid_type(meta, MetaAttribute)):
            raise TypeError("Meta must be None or list of MetaAttribute objects.")
        # TODO(DATAPIPE-2124|justinc): Re-enabled required meta-attribute
        # checking with caching
        # meta_attr_schema_ids = {
        #     meta_attr.schema_id for meta_attr in meta
        # } if meta else set()
        # mandatory_meta_ids = set(
        #     self._schematizer.get_meta_attributes_by_schema_id(schema_id)
        # )
        # if not mandatory_meta_ids.issubset(meta_attr_schema_ids):
        #     raise MissingMetaAttributeException(
        #         schema_id,
        #         meta_attr_schema_ids,
        #         mandatory_meta_ids
        #     )
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
    def keys(self):
        """Currently this support primary keys for flat record
        type avro schema. Support for primary keys in nested
        avro schema will be handled in future versions.
        """
        if self._keys is not None:
            return self._keys
        self._set_keys()
        return self._keys

    def _set_keys(self):
        avro_schema = self._schematizer.get_schema_by_id(self.schema_id)
        self._keys = {
            key: self.payload_data[key] for key in avro_schema.primary_keys
        }

    @property
    def encoded_keys(self):
        writer = _AvroStringStore().get_writer(
            id_key="{0}_{1}".format("keys", self.schema_id),
            avro_schema=self._keys_avro_json
        )
        return writer.encode(message_avro_representation=self.keys)

    def _extract_key_fields(self):
        avro_schema = self._schematizer.get_schema_by_id(
            self.schema_id
        )
        schema_json = avro_schema.schema_json

        fields = schema_json.get('fields', [])
        field_name_to_field = {f['name']: f for f in fields}
        key_fields = [field_name_to_field[pkey] for pkey in avro_schema.primary_keys]
        return key_fields

    @property
    def _keys_avro_json(self):
        return {
            "type": "record",
            "namespace": "yelp.data_pipeline",
            "name": "primary_keys",
            "doc": "Represents primary keys present in Message payload.",
            "fields": self._extract_key_fields()
        }

    @property
    def payload(self):
        return self._avro_payload.payload

    @property
    def payload_data(self):
        return self._avro_payload.payload_data

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

        # TODO(DATAPIPE-416|psuben): Make it so contains_pii is no longer
        # overrideable. Now the pass-in contains_pii is no longer used. Next
        # is to remove it from the function signature altogether.
        if contains_pii is not None:
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn(
                "contains_pii is deprecated. Please stop passing it in.",
                DeprecationWarning
            )
        if topic:
            warnings.simplefilter("always", category=DeprecationWarning)
            warnings.warn("Passing in topics explicitly is deprecated.", DeprecationWarning)
        self._avro_payload = _AvroPayload(
            schema_id=schema_id,
            reader_schema_id=reader_schema_id,
            payload=payload,
            payload_data=payload_data,
            dry_run=dry_run
        )
        self._set_topic(
            topic or str(self._schematizer.get_schema_by_id(schema_id).topic.name)
        )
        self._set_uuid(uuid)
        self._set_timestamp(timestamp)
        self._set_upstream_position_info(upstream_position_info)
        self._set_kafka_position_info(kafka_position_info)
        if keys is not None:
            warnings.simplefilter("always", category=DeprecationWarning)
            warnings.warn("Passing in keys explicitly is deprecated.", DeprecationWarning)
        self._keys = None
        self._set_meta(meta, schema_id)
        self._should_be_encrypted_state = None
        self._encryption_type = None
        self._contains_pii = None

    def _is_valid_optional_type(self, value, typ):
        return value is None or isinstance(value, typ)

    def _any_invalid_type(self, value_list, typ):
        if not value_list:
            return False
        return any(not isinstance(value, typ) for value in value_list)

    def _encrypt_payload_if_necessary(self, payload):
        if self.encryption_type is not None:
            return self._encryption_helper.encrypt_payload(payload)
        return payload

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
        reader_schema_id=None,
        kafka_position_info=None
    ):
        encryption_type = unpacked_message['encryption_type']
        meta = cls._get_unpacked_meta(unpacked_message)
        encryption_meta = cls._pop_encryption_meta(encryption_type, meta)
        payloads = {
            param_name: cls._get_unpacked_decrypted_payload(
                payload,
                encryption_type=encryption_type,
                encryption_meta=encryption_meta
            )
            for param_name, payload in cls._get_all_payloads(
                unpacked_message
            ).iteritems()
        }

        message_params = {
            'uuid': unpacked_message['uuid'],
            'schema_id': unpacked_message['schema_id'],
            'reader_schema_id': reader_schema_id,
            'timestamp': unpacked_message['timestamp'],
            'meta': meta,
            'kafka_position_info': kafka_position_info
        }
        message_params.update(payloads)
        message = cls(**message_params)
        message._should_be_encrypted_state = bool(encryption_type)
        return message

    @classmethod
    def _get_unpacked_meta(cls, unpacked_message):
        return [
            MetaAttribute(schema_id=o['schema_id'], payload=o['payload'])
            for o in unpacked_message['meta']
        ] if unpacked_message['meta'] else None

    @classmethod
    def _get_unpacked_decrypted_payload(
        cls,
        payload,
        encryption_type,
        encryption_meta
    ):
        if not encryption_type:
            return payload

        encryption_helper = EncryptionHelper(encryption_type, encryption_meta)
        return encryption_helper.decrypt_payload(payload)

    @classmethod
    def _pop_encryption_meta(cls, encryption_type, meta):
        if not encryption_type or not meta:
            return None

        encryption_meta = EncryptionHelper.get_encryption_meta_by_encryption_type(
            encryption_type
        )
        for index, meta_attr in enumerate(meta):
            if meta_attr.schema_id == encryption_meta.schema_id:
                target_meta = meta_attr
                meta[index] = meta[-1]
                meta.pop()
                return target_meta
        return None

    @classmethod
    def _get_all_payloads(cls, unpacked_message):
        """Get all the payloads in the message."""
        return {'payload': unpacked_message['payload']}

    def _get_cleaned_pii_data(self, data):
        if not isinstance(data, dict):
            return unicode(type(data))
        return {
            key: self._get_cleaned_pii_data(value)
            for key, value in data.iteritems()
        }

    def reload_data(self):
        """Populate the payload data or the payload if it hasn't done so.
        """
        self._avro_payload.reload_data()

    @property
    def _str_repr(self):
        cleaned_payload_data = self.payload_data
        if self.contains_pii:
            cleaned_payload_data = self._get_cleaned_pii_data(
                self._avro_payload.printable_payload_data
            )
        return {
            'uuid': self.uuid_hex,
            'message_type': self.message_type.name,
            'schema_id': self.schema_id,
            'payload_data': cleaned_payload_data,
            'timestamp': self.timestamp,
            'meta': None if self.meta is None else [m._asdict() for m in self.meta],
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


class RegistrationMessage(Message):
    _message_type = _ProtectedMessageType.registration

    def _get_field_diff(self, field):
        raise InvalidOperation()


class UpdateMessage(Message):
    """Message for update type. This type of message requires previous
    payload in addition to the payload.

    For complete argument docs, see :class:`data_pipeline.message.Message`.

    Args:
        previous_payload (bytes): Avro-encoded message - encoded with schema
            identified by `schema_id`  Required when message type is
            MessageType.update.  Either `previous_payload` or
            `previous_payload_data` must be provided but not both.
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
        self._previous_avro_payload = _AvroPayload(
            schema_id=schema_id,
            reader_schema_id=reader_schema_id,
            payload=previous_payload,
            payload_data=previous_payload_data,
            dry_run=dry_run
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
        return self._previous_avro_payload.payload

    @property
    def previous_payload_data(self):
        return self._previous_avro_payload.payload_data

    @property
    def avro_repr(self):
        repr_dict = super(UpdateMessage, self).avro_repr
        repr_dict['previous_payload'] = self._encrypt_payload_if_necessary(
            self.previous_payload
        )
        return repr_dict

    @classmethod
    def _get_all_payloads(cls, unpacked_message):
        """Get all the payloads in the message."""
        return {
            'payload': unpacked_message['payload'],
            'previous_payload': unpacked_message['previous_payload']
        }

    def reload_data(self):
        """Populate the previous payload data or decode the previous payload
        if it hasn't done so. The payload encoding/payload data decoding is
        taken care of by the `Message.reload` function in the parent class.
        """
        super(UpdateMessage, self).reload_data()
        self._previous_avro_payload.reload_data()

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
            else self._get_cleaned_pii_data(self.previous_payload_data)
        )

        return repr_dict


_message_type_to_class_map = {
    o._message_type.name: o for o in Message.__subclasses__() if o._message_type
}


def create_from_kafka_message(
    kafka_message,
    envelope=None,
    force_payload_decoding=True,
    reader_schema_id=None
):
    """ Build a data_pipeline.message.Message from a yelp_kafka message. If no
    reader schema id is provided, the schema used for encoding will be used for
    decoding.

    Args:
        kafka_message (kafka.common.KafkaMessage): The message info which
            has the topic, partition, offset, key, and value(payload) of
            the received message.
        envelope (Optional[:class:data_pipeline.envelope.Envelope]): Envelope
            instance that unpacks the data pipeline messages.
        force_payload_decoding (Optional[boolean]): If this is set to `True`
            then we will decode the payload/previous_payload immediately.
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
        packed_message=kafka_message,
        envelope=envelope or Envelope(),
        force_payload_decoding=force_payload_decoding,
        kafka_position_info=kafka_position_info,
        reader_schema_id=reader_schema_id
    )


def create_from_offset_and_message(
    offset_and_message,
    force_payload_decoding=True,
    reader_schema_id=None,
    envelope=None
):
    """
    Build a data_pipeline.message.Message from a kafka.common.OffsetAndMessage.
    If no reader schema id is provided, the schema used for encoding will be
    used for decoding.

    Args:
        offset_and_message (kafka.common.OffsetAndMessage): a namedtuple
            containing the offset and message. Message contains magic,
            attributes, keys and values.
        force_payload_decoding (Optional[boolean]): If this is set to `True`
            then we will decode the payload/previous_payload immediately.
            Otherwise the decoding will happen whenever the lazy *_data
            properties are accessed.
        reader_schema_id (Optional[int]): Schema id used to decode the incoming
            kafka message and build data_pipeline.message.Message message.
            Defaults to None.
        envelope (Optional[:class:data_pipeline.envelope.Envelope]): Envelope
            instance that unpacks the data pipeline messages.

    Returns (data_pipeline.message.Message):
        The message object
    """
    return _create_message_from_packed_message(
        packed_message=offset_and_message.message,
        envelope=envelope or Envelope(),
        force_payload_decoding=force_payload_decoding,
        reader_schema_id=reader_schema_id
    )


def _create_message_from_packed_message(
    packed_message,
    envelope,
    force_payload_decoding,
    kafka_position_info=None,
    reader_schema_id=None
):
    """ Builds a data_pipeline.message.Message from packed_message. If no
    reader schema id is provided, the schema used for encoding will be used for
    decoding.

    Args:
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
    unpacked_message = envelope.unpack(packed_message.value)
    message_class = _message_type_to_class_map[unpacked_message['message_type']]
    message = message_class.create_from_unpacked_message(
        unpacked_message=unpacked_message,
        kafka_position_info=kafka_position_info,
        reader_schema_id=reader_schema_id
    )
    if force_payload_decoding:
        # Access the cached, but lazily-calculated, properties
        message.reload_data()
    return message
