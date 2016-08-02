# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from yelp_avro.avro_string_writer import AvroStringWriter

from data_pipeline.meta_attribute import MetaAttribute
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


@pytest.mark.usefixtures("containers")
class TestMetaAttribute(object):

    @pytest.fixture
    def meta_attribute_avro_schema_json(self):
        return {
            "type": "record",
            "namespace": "test_namespace",
            "name": "i_am_so_meta",
            'doc': 'test',
            "fields": [
                {"type": "int", 'doc': 'test', "name": "me_name_meta"}
            ]
        }

    @pytest.fixture
    def meta_attribute_avro_schema(self, meta_attribute_avro_schema_json):
        return get_schematizer().register_schema_from_schema_json(
            namespace="test_namespace",
            source="meta_me_meta",
            schema_json=meta_attribute_avro_schema_json,
            source_owner_email="test@yelp.com",
            contains_pii=False
        )

    @pytest.fixture
    def meta_attribute_payload_data(self):
        return {'me_name_meta': 666}

    @pytest.fixture
    def meta_attribute_payload(
        self,
        meta_attribute_avro_schema_json,
        meta_attribute_payload_data
    ):
        writer = AvroStringWriter(
            schema=meta_attribute_avro_schema_json
        )
        return writer.encode(
            message_avro_representation=meta_attribute_payload_data
        )

    def test_meta_attribute_from_payload_data(
        self,
        meta_attribute_avro_schema,
        meta_attribute_payload_data,
        meta_attribute_payload
    ):
        meta_attribute = MetaAttribute(
            schema_id=meta_attribute_avro_schema.schema_id,
            payload_data=meta_attribute_payload_data
        )
        expected_avro_repr = {
            'schema_id': meta_attribute_avro_schema.schema_id,
            'payload': meta_attribute_payload
        }
        assert meta_attribute.avro_repr == expected_avro_repr

    def test_meta_attribute_from_payload(
        self,
        meta_attribute_avro_schema,
        meta_attribute_payload_data,
        meta_attribute_payload
    ):
        meta_attribute = MetaAttribute(
            schema_id=meta_attribute_avro_schema.schema_id,
            payload=meta_attribute_payload
        )
        expected_avro_repr = {
            'schema_id': meta_attribute_avro_schema.schema_id,
            'payload': meta_attribute_payload
        }
        assert meta_attribute.avro_repr == expected_avro_repr
        assert meta_attribute.payload_data == meta_attribute_payload_data

    @pytest.fixture(params=[
        {'schema_id': 'not_an_int', 'payload': bytes(10)},
        {'schema_id': 10, 'payload_data': None},
        {'schema_id': 10, 'payload': None},
        {'schema_id': 10, 'payload': u'not_bytes'},
        {'schema_id': 10, 'payload_data': 666, 'payload': bytes(10)}
    ])
    def invalid_arguments(self, request):
        return request.param

    def test_create_meta_attr_fails_with_invalid_arguments(self, invalid_arguments):
        with pytest.raises(TypeError):
            MetaAttribute(**invalid_arguments)
