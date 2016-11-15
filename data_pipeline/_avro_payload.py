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

from data_pipeline.config import get_config
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


logger = get_config().logger


class _AvroPayload(object):

    def __init__(
        self,
        schema_id,
        reader_schema_id=None,
        payload=None,
        payload_data=None,
        dry_run=False
    ):
        self._set_schema_id(schema_id)
        self._set_reader_schema_id(reader_schema_id)
        self._set_dry_run(dry_run)
        self._set_payload_or_payload_data(payload, payload_data)

    @property
    def _schematizer(self):
        return get_schematizer()

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
        if (reader_schema_id is not None and
                not isinstance(reader_schema_id, int)):
            raise TypeError("Reader Schema id should be an int")
        self._reader_schema_id = reader_schema_id or self.schema_id

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

    @property
    def printable_payload_data(self):
        return self._get_printable_payload_data(self.payload_data)

    def _get_printable_payload_data(self, data):
        if not isinstance(data, dict):
            return data.encode('hex') if isinstance(data, bytes) else data
        return {
            key: self._get_printable_payload_data(value)
            for key, value in data.iteritems()
        }

    def _set_payload_data(self, payload_data):
        """We shold check to verify that payload data is not None
        payload_data should not necessarily be a dict. example if the schema is something like
        {
          "type": "fixed",
          "size": 16,
          "namespace": "yelp.data_pipeline",
          "name": "initialization_vector",
          "doc": "Serializes an initialization vector for encrypting PII."
        }

        then the corresponding payload data will be 1234123412341234 which is not a dict

        In ideal scenario we should be using avro.io.validate function to validate if
        the given payload_data is a valid datum, but this check happens anyway when we
        encode payload_data with avro_schema.
        """
        if payload_data is None:
            raise TypeError("Payload Data cannot be None")
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
            reader_id_key=self.reader_schema_id,
            writer_id_key=self.schema_id
        )

    def reload_data(self):
        """Populate the payload data or the payload if it hasn't done so.
        """
        self._set_payload_data_if_necessary(self._payload)
        self._set_payload_if_necessary(self._payload_data)
