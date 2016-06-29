# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.config import get_config
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


logger = get_config().logger


class _AvroPayload(object):

    def __init__(
        self,
        schema_id,
        topic=None,
        payload=None,
        payload_data=None,
        dry_run=False
    ):
        self._set_schema_id(schema_id)
        self._set_topic(
            topic or str(self.schematizer.get_schema_by_id(schema_id).topic.name)
        )
        self._set_dry_run(dry_run)
        self._set_payload_or_payload_data(payload, payload_data)

    @property
    def schematizer(self):
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
    def dry_run(self):
        return self._dry_run

    def _set_dry_run(self, dry_run):
        self._dry_run = dry_run

    def _set_payload_or_payload_data(self, payload, payload_data):
        # payload or payload_data are lazily constructed only on request
        is_not_none_payload = payload is not None
        is_not_none_payload_data = payload_data is not None

        if is_not_none_payload == is_not_none_payload_data:
            raise TypeError("Exactly one of payload and payload_data must be set.")
        if is_not_none_payload:
            self._set_payload(payload)
        elif is_not_none_payload_data:
            self._set_payload_data(payload_data)

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
        self._payload_data = payload_data
        self._payload = None  # force payload to be re-encoded

    def _set_payload_data_if_necessary(self, payload):
        if self._payload_data is None:
            self._payload_data = self._decode_payload(payload)

    def _set_payload_if_necessary(self, payload_data):
        if self._payload is None:
            self._payload = self._encode_payload_data(payload_data)

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

    @property
    def _avro_string_writer(self):
        """get the writer from store if already exists"""
        return _AvroStringStore().get_writer(self.schema_id)

    @property
    def _avro_string_reader(self):
        """get the reader from store if already exists"""
        return _AvroStringStore().get_reader(
            reader_schema_id=self.schema_id,
            writer_schema_id=self.schema_id
        )

    def reload_data(self):
        """Populate the payload data or the payload if it hasn't done so.
        """
        self._set_payload_data_if_necessary(self._payload)
        self._set_payload_if_necessary(self._payload_data)
