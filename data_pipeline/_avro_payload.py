# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


logger = get_config().logger


class _AvroPayload(object):

    _fast_uuid = FastUUID()
    """UUID generator - this isn't a @cached_property so it can be serialized"""

    def __init__(
        self,
        schema_id,
        topic=None,
        payload=None,
        payload_data=None,
        uuid=None,
        timestamp=None,
        dry_run=False
    ):
        self._set_schema_id(schema_id)
        self._set_topic(
            topic or str(self._schematizer.get_schema_by_id(schema_id).topic.name)
        )
        self._set_contains_pii()
        self._set_uuid(uuid)
        self._set_timestamp(timestamp)
        self._set_dry_run(dry_run)
        self._set_payload_or_payload_data(payload, payload_data)
        if topic:
            logger.debug(
                "Overriding message topic: {} for schema {}.".format(topic, schema_id)
            )
        self._contains_pii = None

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
    def timestamp(self):
        return self._timestamp

    def _set_timestamp(self, timestamp):
        if timestamp is None:
            timestamp = int(time.time())
        self._timestamp = timestamp

    @property
    def dry_run(self):
        return self._dry_run

    def _set_dry_run(self, dry_run):
        self._dry_run = dry_run

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
