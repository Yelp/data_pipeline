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

from random import randint

from data_pipeline.message import CreateMessage
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class SchemaFactory(object):
    SOURCE_SCHEMA = '''
    {
        "type": "record",
        "namespace": "test_namespace",
        "doc": "test_doc",
        "name": "source_schema",
        "fields": [
            {"type": "int","name": "original", "doc": "test_doc"}
        ]
    }
    '''

    @classmethod
    def get_schema_json(cls):
        return get_schematizer().register_schema(
            schema_str=cls.SOURCE_SCHEMA,
            namespace='test_namespace',
            source="test_source_{}".format(randint(0, 100)),
            source_owner_email='test@yelp.com',
            contains_pii=False
        )

    @classmethod
    def get_payload_data(cls):
        return {"original": randint(0, 1000000)}


class MessageFactory(object):

    @classmethod
    def create_message_with_payload_data(self):
        return CreateMessage(
            schema_id=SchemaFactory.get_schema_json().schema_id,
            payload_data=SchemaFactory.get_payload_data()
        )
