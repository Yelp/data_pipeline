# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from random import randint

from data_pipeline.message import CreateMessage
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class SchemaFactory(object):
    SOURCE_SCHEMA = '''
    {
        "type":"record",
        "namespace":"test_namespace",
        "name":"source_schema",
        "fields":[
            {"type":"int","name":"original"}
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
