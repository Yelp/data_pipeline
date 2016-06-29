# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline._encryption_helper import _AVSCInfo
from data_pipeline._encryption_helper import _AVSCStore
from data_pipeline.meta_attribute import MetaAttribute


class TestMetaAttribute(object):

    @pytest.fixture
    def meta_attr_payload_data(self):
        return {'me_name_meta': 666}

    @pytest.fixture
    def avro_schema_json(self):
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
    def avro_schema_info(self):
        return _AVSCInfo(
            id=11,
            avsc_file_path='mock_avsc_file_path',
            namespace='yelp.meta_all_things',
            source='meta_me_meta',
            source_owner_email='meta_handler@yelp.com',
            contains_pii=False
        )

    @pytest.fixture
    def new_meta_attribute(
        self,
        avro_schema_json,
        avro_schema_info,
        meta_attr_payload_data
    ):
        with mock.patch.object(
            _AVSCStore,
            '_load_avro_schema_file',
            return_value=avro_schema_json
        ):
            schema_id = _AVSCStore().get_schema_id(avro_schema_info)
            return MetaAttribute(
                schema_id=schema_id,
                payload_data=meta_attr_payload_data
            )

    @pytest.fixture(params=[
        {'schema_id': 10},
        {'payload': bytes(10)}
    ])
    def invalid_arg_value(self, request):
        return request.param

    def test_create_meta_attr_fails_without_both_args(self, invalid_arg_value, containers):
        with pytest.raises(TypeError):
            MetaAttribute(**invalid_arg_value)

    @pytest.fixture(params=[
        {'schema_id': 'not_an_int', 'payload': bytes(10)},
        {'schema_id': 10, 'payload': u'not_bytes'}
    ])
    def invalid_arg_type(self, request):
        return request.param

    def test_create_meta_attr_fails_with_invalid_arg_type(self, invalid_arg_type, containers):
        with pytest.raises(TypeError):
            MetaAttribute(**invalid_arg_type)

    def test_meta_attribute_encoding(self, new_meta_attribute, containers):
        assert isinstance(new_meta_attribute.avro_repr, dict)
        assert isinstance(new_meta_attribute.avro_repr['schema_id'], int)
        assert isinstance(new_meta_attribute.avro_repr['payload'], bytes)

    def test_meta_attribute_decoding(self, new_meta_attribute, meta_attr_payload_data, containers):
        meta_attr_avro_repr = new_meta_attribute.avro_repr
        decoded_meta_attr = MetaAttribute(
            schema_id=meta_attr_avro_repr['schema_id'],
            payload=meta_attr_avro_repr['payload']
        )
        assert decoded_meta_attr.payload_data == meta_attr_payload_data
