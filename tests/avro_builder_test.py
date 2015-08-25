# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from avro import schema

from data_pipeline import avro_builder


class TestAvroSchemaBuilder(object):

    @pytest.fixture
    def builder(self):
        return avro_builder.AvroSchemaBuilder()

    @property
    def name(self):
        return 'foo'

    @property
    def namespace(self):
        return 'ns'

    @property
    def aliases(self):
        return ['new_foo']

    @property
    def doc(self):
        return 'sample doc'

    @property
    def metadata(self):
        return {'key1': 'val1', 'key2': 'val2'}

    @property
    def enum_symbols(self):
        return ['a', 'b']

    @property
    def fixed_size(self):
        return 16

    @property
    def another_name(self):
        return 'bar'

    @property
    def invalid_schemas(self):
        undefined_schema_name = 'unknown'
        yield undefined_schema_name

        non_avro_schema = {'foo': 'bar'}
        yield non_avro_schema

        named_schema_without_name = {'name': '', 'type': 'fixed', 'size': 16}
        yield named_schema_without_name

        invalid_schema = {'name': 'foo', 'type': 'enum', 'symbols': ['a', 'a']}
        yield invalid_schema

        none_schema = None
        yield none_schema

    @property
    def invalid_names(self):
        missing_name = None
        yield missing_name

        reserved_name = 'int'
        yield reserved_name

        non_string_name = 100
        yield non_string_name

    @property
    def duplicate_name_err(self):
        return '"{0}" is already in use.'

    def test_create_primitive_types(self, builder):
        assert 'null' == builder.create_null()
        assert 'boolean' == builder.create_boolean()
        assert 'int' == builder.create_int()
        assert 'long' == builder.create_long()
        assert 'float' == builder.create_float()
        assert 'double' == builder.create_double()
        assert 'bytes' == builder.create_bytes()
        assert 'string' == builder.create_string()

    def test_create_enum(self, builder):
        actual_json = builder.begin_enum(self.name, self.enum_symbols).end()
        expected_json = {
            'type': 'enum',
            'name': self.name,
            'symbols': self.enum_symbols
        }
        assert expected_json == actual_json

    def test_create_enum_with_optional_attributes(self, builder):
        actual_json = builder.begin_enum(
            self.name,
            self.enum_symbols,
            self.namespace,
            self.aliases,
            self.doc,
            **self.metadata
        ).end()

        expected_json = {
            'type': 'enum',
            'name': self.name,
            'symbols': self.enum_symbols,
            'namespace': self.namespace,
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_json.update(self.metadata)
        assert expected_json == actual_json

    def test_create_enum_with_invalid_name(self, builder):
        for invalid_name in self.invalid_names:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_enum(invalid_name, self.enum_symbols).end()

    def test_create_enum_with_dup_name(self, builder):
        with pytest.raises_regexp(
                schema.SchemaParseException,
                self.duplicate_name_err.format(self.name)
        ):
            builder.begin_record(self.name)
            builder.add_field(
                self.another_name,
                builder.begin_enum(self.name, self.enum_symbols).end()
            )
            builder.end()

    def test_create_enum_with_invalid_symbols(self, builder):
        def single_test_create_enum_with_invalid_symbols(invalid_symbols):
            builder.clear()
            with pytest.raises(schema.AvroException):
                builder.begin_enum(self.name, invalid_symbols).end()
        single_test_create_enum_with_invalid_symbols(None)
        single_test_create_enum_with_invalid_symbols('')
        single_test_create_enum_with_invalid_symbols('a')
        single_test_create_enum_with_invalid_symbols(['a', 1])
        single_test_create_enum_with_invalid_symbols([1, 2, 3])
        single_test_create_enum_with_invalid_symbols(['a', 'a'])

    def test_create_fixed(self, builder):
        actual_json = builder.begin_fixed(self.name, self.fixed_size).end()
        expected_json = {
            'type': 'fixed',
            'name': self.name,
            'size': self.fixed_size
        }
        assert expected_json == actual_json

    def test_create_fixed_with_optional_attributes(self, builder):
        actual_json = builder.begin_fixed(
            self.name,
            self.fixed_size,
            self.namespace,
            self.aliases,
            **self.metadata
        ).end()
        expected_json = {
            'type': 'fixed',
            'name': self.name,
            'size': self.fixed_size,
            'namespace': self.namespace,
            'aliases': self.aliases,
        }
        expected_json.update(self.metadata)
        assert expected_json == actual_json

    def test_create_fixed_with_invalid_name(self, builder):
        for invalid_name in self.invalid_names:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_fixed(invalid_name, self.fixed_size).end()

    def test_create_fixed_with_dup_name(self, builder):
        with pytest.raises_regexp(
                schema.SchemaParseException,
                self.duplicate_name_err.format(self.name)
        ):
            builder.begin_record(self.name)
            builder.add_field(
                self.another_name,
                builder.begin_fixed(self.name, self.fixed_size).end()
            )
            builder.end()

    def test_create_fixed_with_invalid_size(self, builder):
        def single_test_create_fixed_with_invalid_size(invalid_size):
            builder.clear()
            with pytest.raises(schema.AvroException):
                builder.begin_fixed(self.name, invalid_size).end()
        single_test_create_fixed_with_invalid_size(None)
        single_test_create_fixed_with_invalid_size('ten')

    def test_create_array(self, builder):
        actual_json = builder.begin_array(builder.create_int()).end()
        expected_json = {'type': 'array', 'items': 'int'}
        assert expected_json == actual_json

    def test_create_array_with_optional_attributes(self, builder):
        actual_json = builder.begin_array(
            builder.create_int(),
            **self.metadata
        ).end()

        expected_json = {'type': 'array', 'items': 'int'}
        expected_json.update(self.metadata)

        assert expected_json == actual_json

    def test_create_array_with_complex_type(self, builder):
        actual_json = builder.begin_array(
            builder.begin_enum(self.name, self.enum_symbols).end()
        ).end()
        expected_json = {
            'type': 'array',
            'items': {
                'type': 'enum',
                'name': self.name,
                'symbols': self.enum_symbols
            }
        }
        assert expected_json == actual_json

    def test_create_array_with_invalid_items_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.AvroException):
                builder.begin_array(invalid_schema).end()

    def test_create_map(self, builder):
        actual_json = builder.begin_map(builder.create_string()).end()
        expected_json = {'type': 'map', 'values': 'string'}
        assert expected_json == actual_json

    def test_create_map_with_optional_attributes(self, builder):
        actual_json = builder.begin_map(
            builder.create_string(),
            **self.metadata
        ).end()
        expected_json = {'type': 'map', 'values': 'string'}
        expected_json.update(self.metadata)
        assert expected_json == actual_json

    def test_create_map_with_complex_type(self, builder):
        actual_json = builder.begin_map(
            builder.begin_fixed(self.name, self.fixed_size).end()
        ).end()
        expected_json = {
            'type': 'map',
            'values': {
                'type': 'fixed',
                'name': self.name,
                'size': self.fixed_size
            }
        }
        assert expected_json == actual_json

    def test_create_map_with_invalid_values_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.AvroException):
                builder.begin_map(invalid_schema).end()

    def test_create_record(self, builder):
        builder.begin_record(self.name)
        builder.add_field(
            'bar1',
            builder.create_int()
        )
        builder.add_field(
            'bar2',
            builder.begin_map(builder.create_double()).end()
        )
        actual_json = builder.end()
        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [
                {'name': 'bar1', 'type': 'int'},
                {'name': 'bar2', 'type': {'type': 'map', 'values': 'double'}}
            ]
        }
        assert expected_json == actual_json

    def test_create_record_with_optional_attributes(self, builder):
        builder.begin_record(
            self.name,
            namespace=self.namespace,
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        )
        builder.add_field(
            self.another_name,
            builder.create_int()
        )
        actual_json = builder.end()
        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': self.another_name, 'type': 'int'}],
            'namespace': self.namespace,
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_json.update(self.metadata)
        assert expected_json == actual_json

    def test_create_field_with_optional_attributes(self, builder):
        builder.begin_record(self.name)
        builder.add_field(
            self.another_name,
            builder.create_boolean(),
            has_default=True,
            default_value=True,
            sort_order='ascending',
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        )
        actual_json = builder.end()
        expected_field = {
            'name': self.another_name,
            'type': 'boolean',
            'default': True,
            'order': 'ascending',
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_field.update(self.metadata)
        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [expected_field]
        }
        assert expected_json == actual_json

    def test_create_record_with_no_field(self, builder):
        actual_json = builder.begin_record(self.name).end()
        expected_json = {'type': 'record', 'name': self.name, 'fields': []}
        assert expected_json == actual_json

    def test_create_record_with_invalid_name(self, builder):
        for invalid_name in self.invalid_names:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_record(invalid_name)
                builder.add_field(
                    self.another_name,
                    builder.create_int()
                )
                builder.end()

    def test_create_record_with_dup_name(self, builder):
        with pytest.raises_regexp(
                schema.SchemaParseException,
                self.duplicate_name_err.format(self.name)
        ):
            builder.begin_record(self.another_name)
            builder.add_field(
                'bar1',
                builder.begin_enum(self.name, self.enum_symbols).end()
            )
            builder.add_field(
                'bar2',
                builder.begin_record(self.name).end()
            )
            builder.end()

    def test_create_record_with_dup_field_name(self, builder):
        with pytest.raises_regexp(
                schema.SchemaParseException,
                "{0} already in use.".format(self.another_name)
        ):
            builder.begin_record(self.name)
            builder.add_field(
                self.another_name,
                builder.create_int()
            )
            builder.add_field(
                self.another_name,
                builder.create_string()
            )
            builder.end()

    def test_create_field_with_invalid_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_record(self.name)
                builder.add_field(
                    self.another_name,
                    invalid_schema
                )
                builder.end()

    def test_create_field_with_invalid_sort_order(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_record(self.name)
            builder.add_field(
                self.another_name,
                builder.create_int(),
                sort_order='asc'
            )
            builder.end()

    def test_create_union(self, builder):
        actual_json = builder.begin_union(
            builder.create_null(),
            builder.create_string(),
            builder.begin_enum(self.name, self.enum_symbols).end()
        ).end()
        expected_json = [
            'null',
            'string',
            {'type': 'enum', 'name': self.name, 'symbols': self.enum_symbols}
        ]
        assert expected_json == actual_json

    def test_create_union_with_empty_sub_schemas(self, builder):
        actual_json = builder.begin_union().end()
        expected_json = []
        assert expected_json == actual_json

    def test_create_union_with_nested_union_schema(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.begin_union(builder.create_int()).end()
            ).end()

    def test_create_union_with_invalid_schema(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_union(invalid_schema).end()

    def test_create_union_with_dup_primitive_schemas(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.create_int(),
                builder.create_int()
            ).end()

    def test_create_union_with_dup_named_schemas(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.begin_enum(self.name, self.enum_symbols).end(),
                builder.begin_fixed(self.name, self.fixed_size).end()
            ).end()

    def test_create_union_with_dup_complex_schemas(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.begin_map(builder.create_int()).end(),
                builder.begin_map(builder.create_int()).end()
            ).end()

    def test_create_nullable_type(self, builder):
        # non-union schema type
        actual_json = builder.begin_nullable_type(
            builder.create_int()
        ).end()
        expected_json = ['null', 'int']
        assert expected_json == actual_json

        # union schema type
        actual_json = builder.begin_nullable_type(
            [builder.create_int()]
        ).end()
        expected_json = ['null', 'int']
        assert expected_json == actual_json

    def test_create_nullable_type_with_default_value(self, builder):
        # non-union schema type
        actual_json = builder.begin_nullable_type(
            builder.create_int(),
            10
        ).end()
        expected_json = ['int', 'null']
        assert expected_json == actual_json

        # union schema type
        actual_json = builder.begin_nullable_type(
            [builder.create_int()],
            10
        ).end()
        expected_json = ['int', 'null']
        assert expected_json == actual_json

    def test_create_nullable_type_with_null_type(self, builder):
        actual_json = builder.begin_nullable_type(
            builder.create_null()
        ).end()
        expected_json = 'null'
        assert expected_json == actual_json

    def test_create_nullable_type_with_nullable_type(self, builder):
        actual_json = builder.begin_nullable_type(
            builder.begin_union(
                builder.create_null(),
                builder.create_long()
            ).end(),
            10
        ).end()
        expected_json = ['null', 'long']
        assert expected_json == actual_json

    def test_create_nullable_type_with_invalid_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_nullable_type(invalid_schema)

    def test_create_schema_with_preloaded_json(self, builder):
        schema_json = {
            'type': 'record',
            'name': self.name,
            'fields': [
                {'name': 'field', 'type': {'type': 'map', 'values': 'double'}}
            ]
        }
        builder.begin_with_schema_json(schema_json)
        builder.add_field(
            'field_new',
            builder.create_int()
        )
        actual_json = builder.end()
        expected_json = schema_json.copy()
        expected_json['fields'].append({'name': 'field_new', 'type': 'int'})
        assert expected_json == actual_json

    def test_removed_field(self, builder):
        builder.begin_record(self.name)
        builder.add_field('bar1', builder.create_int())
        builder.add_field('bar2', builder.create_int())
        builder.remove_field('bar1')
        actual_json = builder.end()
        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'bar2', 'type': 'int'}]
        }
        assert expected_json == actual_json

    def test_removed_nonexistent_field(self, builder):
        schema_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'bar2', 'type': 'int'}]
        }
        with pytest.raises(avro_builder.AvroBuildInvalidOperation):
            builder.begin_with_schema_json(schema_json)
            builder.remove_field('bar1')
            builder.end()
