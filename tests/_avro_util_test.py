# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from avro.io import AvroTypeException

from data_pipeline._avro_util import _avro_primitive_type_to_example_value
from data_pipeline._avro_util import AvroStringReader
from data_pipeline._avro_util import AvroStringWriter
from data_pipeline._avro_util import generate_payload_data
from data_pipeline._avro_util import get_avro_schema_object


class TestGeneratePayloadData(object):
    @pytest.fixture
    def test_schema(self):
        return get_avro_schema_object(
            '''{"type": "record", "name": "test_record", "fields":[
            {"type": ["null","int"], "name": "union_field"},
            {"type": ["null","int"], "name": "union_field_null", "default": null},
            {"type": ["null","int"], "name": "union_field_101", "default": 101},
            {"type": "boolean", "name": "bool_field"},
            {"type": "boolean", "name": "bool_field_F", "default": false},
            {"type": "string", "name": "string_field"},
            {"type": "string", "name": "string_field_foo", "default": "foo❤"},
            {"type": "bytes", "name": "bytes_field"},
            {"type": "bytes", "name": "bytes_field_bar", "default": "bar"},
            {"type": "int", "name": "int_field"},
            {"type": "int", "name": "int_field_1", "default": 1},
            {"type": "long", "name": "long_field"},
            {"type": "long", "name": "long_field_42", "default": 42},
            {"type": "float", "name": "float_field"},
            {"type": "float", "name": "float_field_p75", "default": 0.75},
            {"type": "double", "name": "double_field"},
            {"type": "double", "name": "double_field_pi", "default": 3.14}]}'''
        )

    def test_payload_no_fields_filled(self, test_schema):
        expected_data = {
            "union_field": _avro_primitive_type_to_example_value['null'],
            "union_field_null": None,
            "union_field_101": 101,
            "bool_field": _avro_primitive_type_to_example_value['boolean'],
            "bool_field_F": False,
            "string_field": _avro_primitive_type_to_example_value['string'],
            "string_field_foo": 'foo❤',
            "bytes_field": _avro_primitive_type_to_example_value['bytes'],
            "bytes_field_bar": b'bar',
            "int_field": _avro_primitive_type_to_example_value['int'],
            "int_field_1": 1,
            "long_field": _avro_primitive_type_to_example_value['long'],
            "long_field_42": 42,
            "float_field": _avro_primitive_type_to_example_value['float'],
            "float_field_p75": 0.75,
            "double_field": _avro_primitive_type_to_example_value['double'],
            "double_field_pi": 3.14
        }
        data = generate_payload_data(test_schema)
        assert data == expected_data

    def test_payload_all_fields_filled(self, test_schema):
        expected_data = {
            "union_field": 101010,
            "union_field_null": 101010,
            "union_field_101": None,
            "bool_field": False,
            "bool_field_F": True,
            "string_field": 'wow❤wow!',
            "string_field_foo": '❤super!',
            "bytes_field": b'do the robot',
            "bytes_field_bar": b'noooooooo!',
            "int_field": 8,
            "int_field_1": 0,
            "long_field": 10,
            "long_field_42": 999,
            "float_field": 0.1,
            "float_field_p75": 0.75,
            "double_field": 0.3,
            "double_field_pi": 0.4
        }
        data = generate_payload_data(test_schema, expected_data)
        assert data == expected_data

    def test_payload_valid_for_writing_reading(self, test_schema):
        payload_data = generate_payload_data(test_schema)
        avro_schema = get_avro_schema_object(test_schema)
        writer = AvroStringWriter(schema=avro_schema)
        reader = AvroStringReader(
            reader_schema=avro_schema,
            writer_schema=avro_schema
        )
        encoded_payload = writer.encode(payload_data)
        decoded_payload = reader.decode(encoded_payload)
        assert decoded_payload == payload_data

    def test_rejects_empty_avro_representation_for_writing(self, test_schema):
        avro_schema = get_avro_schema_object(test_schema)
        writer = AvroStringWriter(schema=avro_schema)
        with pytest.raises(AvroTypeException):
            writer.encode(message_avro_representation=None)

    def test_rejects_empty_encoded_message_for_reading(self, test_schema):
        avro_schema = get_avro_schema_object(test_schema)
        reader = AvroStringReader(
            reader_schema=avro_schema,
            writer_schema=avro_schema
        )
        with pytest.raises(TypeError):
            reader.decode(encoded_message=None)
