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

from data_pipeline._avro_payload import _AvroPayload


class MetaAttribute(object):
    """Messages flowing through data pipeline can contain an
    additional array of avro encoded payloads under the “meta” key.
    These avro encoded payloads are known as Meta Attributes within the
    data pipeline domain. Meta Attributes may contains additional information
    alongside messages originating from data pipeline producer.

    For example for messages coming from metrics source within yelp_web_metric
    namespace, we could include the web request itself as one of the
    Meta Attribute in that message.

    1. Get a new meta attribute object from `schema_id` and `payload_data`

    Example:

        MetaAttribute(
            schema_id=schema_id,
            payload_data={'cluster_name': 'cluster1',
            'log_file': 'binlog.0001', 'log_pos': 12332})

    2. Recover a MetaAttribute object from serialized MetaAttribute payload.
    This will be usefull specifically in the case where we want to reconstruct
    MetaAttribute object from serialized payload coming out of Kafka.

    Example:

        MetaAttribute(schema_id=schema_id,payload=byte(10))

    Args:
        schema_id (int): Identifies the schema used to encode the payload.
        payload (bytes): Avro-encoded meta attribute - encoded with schema identified
            by `schema_id`. Either `payload` or `payload_data` must be
            provided but not both.
        payload_data: The contents of meta attribute, which will be
            encoded with schema identified by `schema_id`. Either `payload` or
            `payload_data` must be provided but not both. Type of payload_data
            should match the avro type specified schema.
        dry_run (boolean): When set to True, MetaAttribute will return a string
            representation of the payload, instead of the avro encoded MetaAttribute.
            Defaults to False.
    """

    def __init__(
        self,
        schema_id,
        payload=None,
        payload_data=None,
        dry_run=False
    ):
        self._avro_payload = _AvroPayload(
            schema_id=schema_id,
            payload=payload,
            payload_data=payload_data,
            dry_run=dry_run
        )

    @property
    def payload(self):
        return self._avro_payload.payload

    @property
    def payload_data(self):
        return self._avro_payload.payload_data

    @property
    def schema_id(self):
        return self._avro_payload.schema_id

    @property
    def avro_repr(self):
        return {
            'schema_id': self.schema_id,
            'payload': self.payload
        }

    def _asdict(self):
        """ Helper method for simplejson encoding in the Tailer
        """
        return {
            'schema_id': self.schema_id,
            'payload_data': self._avro_payload.printable_payload_data
        }

    def __repr__(self):
        return '{}'.format(self._asdict())
