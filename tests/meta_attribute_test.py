# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from cached_property import cached_property

from data_pipeline.meta_attribute import MetaAttribute


class TestMetaAttribute(object):

    @pytest.fixture
    def meta_attr_payload(self):
        return {'me_name_meta': 666}

    @pytest.fixture
    def avro_schema_json(self):
        return {
            "type": "record",
            "namespace": "test_namespace",
            "name": "i_am_so_meta",
            "fields": [
                {"type": "int", "name": "me_name_meta"}
            ]
        }

    @pytest.fixture
    def new_meta_attribute(
        self,
        schematizer_client,
        avro_schema_json,
        meta_attr_payload
    ):

        class NewMetaAttribute(MetaAttribute):

            @cached_property
            def schematizer(self):
                return schematizer_client

            @cached_property
            def owner_email(self):
                return 'meta_handler@yelp.com'

            @cached_property
            def source(self):
                return 'meta_me_meta'

            @cached_property
            def namespace(self):
                return 'yelp.meta_all_things'

            @cached_property
            def contains_pii(self):
                return False

            @cached_property
            def avro_schema(self):
                return avro_schema_json

            @cached_property
            def payload(self):
                return meta_attr_payload

        return NewMetaAttribute()

    @pytest.fixture(params=[
        {'schema_id': 10},
        {'encoded_payload': bytes(10)}
    ])
    def invalid_arg_value(self, request):
        return request.param

    def test_create_meta_attr_fails_without_both_args(self, invalid_arg_value):
        with pytest.raises(ValueError):
            MetaAttribute(**invalid_arg_value)

    @pytest.fixture(params=[
        {'schema_id': 'not_an_int', 'encoded_payload': bytes(10)},
        {'schema_id': 10, 'encoded_payload': u'not_bytes'}
    ])
    def invalid_arg_type(self, request):
        return request.param

    def test_create_meta_attr_fails_with_invalid_arg_type(self, invalid_arg_type):
        with pytest.raises(TypeError):
            MetaAttribute(**invalid_arg_type)

    def test_meta_attribute_encoding(self, new_meta_attribute):
        assert isinstance(new_meta_attribute.avro_repr, dict)
        assert isinstance(new_meta_attribute.avro_repr['schema_id'], int)
        assert isinstance(new_meta_attribute.avro_repr['payload'], bytes)

    def test_meta_attribute_decoding(self, new_meta_attribute, meta_attr_payload):
        meta_attr_avro_repr = new_meta_attribute.avro_repr
        decoded_meta_attr = MetaAttribute(
            schema_id=meta_attr_avro_repr['schema_id'],
            encoded_payload=meta_attr_avro_repr['payload']
        )
        assert decoded_meta_attr.payload == meta_attr_payload
