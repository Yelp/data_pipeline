# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import simplejson
from cached_property import cached_property

from data_pipeline.meta_attribute import MetaAttribute


class TestMetaAttribute(object):

    @pytest.fixture(params=[
        {'me_name_meta': 666}
    ])
    def meta_attr_payload(self, request):
        return request.param

    @pytest.fixture
    def avr_schema_json(self):
        schema_str = '''
        {
            "type":"record",
            "namespace":"test_namespace",
            "name":"i_am_so_meta",
            "fields":[
                {"type":"int", "name":"me_name_meta"}
            ]
        }
        '''
        return simplejson.loads(schema_str)

    @pytest.fixture
    def new_meta_attribute(self, schematizer_client, avr_schema_json, meta_attr_payload):

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
            def base_schema_id(self):
                return 0

            @cached_property
            def avro_schema(self):
                return avr_schema_json

            @cached_property
            def payload(self):
                return meta_attr_payload

        return NewMetaAttribute()

    def test_meta_attr_decoding_fails_without_dict(self):
        with pytest.raises(TypeError):
            MetaAttribute(unpack_from_meta_attr='not_a_dict')

    def test_meta_attr_decoding_fails_with_incorrect_dict(self):
        dict_without_schema_id_key = {
            'not_schema_id': 1,
            'payload': bytes(10)
        }
        with pytest.raises(ValueError):
            MetaAttribute(unpack_from_meta_attr=dict_without_schema_id_key)

    def test_meta_attribute_encoding(self, new_meta_attribute):
        assert isinstance(new_meta_attribute.avro_repr, dict)
        assert isinstance(new_meta_attribute.avro_repr['schema_id'], int)
        assert isinstance(new_meta_attribute.avro_repr['payload'], bytes)

    def test_meta_attribute_decoding(self, new_meta_attribute, meta_attr_payload):
        meta_attr_avro_repr = new_meta_attribute.avro_repr
        decoded_meta_attr = MetaAttribute(unpack_from_meta_attr=meta_attr_avro_repr)
        assert decoded_meta_attr.payload == meta_attr_payload
