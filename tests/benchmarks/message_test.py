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

import pytest

from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.message import CreateMessage
from tests.factories.base_factory import SchemaFactory


@pytest.mark.usefixtures(
    "config_benchmark_containers_connections"
)
@pytest.mark.benchmark
class TestBenchMessage(object):

    def test_create_message(self, benchmark):

        def create_message(schema_id, payload_data):
            CreateMessage(
                schema_id=schema_id,
                payload_data=payload_data
            )

        def setup():
            schema_id = SchemaFactory.get_schema_json().schema_id
            payload_data = SchemaFactory.get_payload_data()
            return [schema_id, payload_data], {}
        benchmark.pedantic(create_message, setup=setup, rounds=1000)

    def test_encode_message(self, benchmark):

        def setup():
            schema_id = SchemaFactory.get_schema_json().schema_id
            payload_data = SchemaFactory.get_payload_data()

            return [schema_id, payload_data], {}

        def encode_message(schema_id, payload_data):
            _AvroStringStore().get_writer(schema_id).encode(
                message_avro_representation=payload_data
            )

        benchmark.pedantic(encode_message, setup=setup, rounds=1000)

    def test_decode_message(self, benchmark):

        def setup():
            schema_id = SchemaFactory.get_schema_json().schema_id
            payload_data = SchemaFactory.get_payload_data()
            payload = _AvroStringStore().get_writer(schema_id).encode(
                message_avro_representation=payload_data
            )

            return [schema_id, payload], {}

        def decode_message(schema_id, payload):
            _AvroStringStore().get_reader(
                reader_id_key=schema_id,
                writer_id_key=schema_id
            ).decode(
                encoded_message=payload
            )

        benchmark.pedantic(decode_message, setup=setup, rounds=1000)
