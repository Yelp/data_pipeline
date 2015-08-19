# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

import pytest

from data_pipeline.tools.redshift_sql_to_avsc import _sanitize_line
from data_pipeline.tools.redshift_sql_to_avsc \
    import RedshiftFieldLineToAvroFieldConverter
from data_pipeline.tools.redshift_sql_to_avsc \
    import RedshiftSQLToAVSCConverter


@pytest.fixture(
    params=[
        [],
        ['field_a'],
        ['field_a', 'field_b'],
        ['field_b', 'field_a', 'field_c'],
    ]
)
def pkeys(request):
    return copy.deepcopy(request.param)


@pytest.fixture
def field_fixtures(pkeys):
    fixtures = [
        {
            'name': 'field_a',
            'sql_line': 'field_a char(18) NOT NULL encode lzo,',
            'sql_type': 'char',
            'sql_width': 18,
            'avro_type': 'string',
            'avro_meta': {'fixlen': 18},
            'nullable': False
        },
        {
            'name': 'field_b',
            'sql_line': '  field_b   varchar ( 320 )   encode lzo  ,  ',
            'sql_type': 'varchar',
            'sql_width': 320,
            'avro_type': ['string', 'null'],
            'avro_meta': {'maxlen': 320},
            'nullable': True
        },
        {
            'name': 'field_c',
            'sql_line': 'field_c integer,',
            'sql_type': 'integer',
            'sql_width': None,
            'avro_type': ['int', 'null'],
            'avro_meta': {},
            'nullable': True
        },
        {
            'name': 'field_d',
            'sql_line': 'field_d int   NOT    NULL,',
            'sql_type': 'int',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_e',
            'sql_line': 'field_e int4 NOT NULL,',
            'sql_type': 'int4',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_f',
            'sql_line': 'field_f SMALLINT NOT NULL,',
            'sql_type': 'smallint',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_g',
            'sql_line': 'field_g int2 NOT NULL,',
            'sql_type': 'int2',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_h',
            'sql_line': 'field_h bigint,',
            'sql_type': 'bigint',
            'sql_width': None,
            'avro_type': ['long', 'null'],
            'avro_meta': {},
            'nullable': True
        },
        {
            'name': 'field_i',
            'sql_line': 'field_i INT8,',
            'sql_type': 'int8',
            'sql_width': None,
            'avro_type': ['long', 'null'],
            'avro_meta': {},
            'nullable': True
        },
        {
            'name': 'field_j',
            'sql_line': 'field_j real NULL,',
            'sql_type': 'real',
            'sql_width': None,
            'avro_type': ['float', 'null'],
            'avro_meta': {},
            'nullable': True
        },
        {
            'name': 'field_k',
            'sql_line': '   field_k    float4    not    null   ,   ',
            'sql_type': 'float4',
            'sql_width': None,
            'avro_type': 'float',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_l',
            'sql_line': 'field_l double not null,',
            'sql_type': 'double',
            'sql_width': None,
            'avro_type': 'double',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_m',
            'sql_line': 'field_m   float8      null,',
            'sql_type': 'float8',
            'sql_width': None,
            'avro_type': ['double', 'null'],
            'avro_meta': {},
            'nullable': True
        },
        {
            'name': 'field_n',
            'sql_line': '   field_n FLOAT,',
            'sql_type': 'float',
            'sql_width': None,
            'avro_type': ['double', 'null'],
            'avro_meta': {},
            'nullable': True
        },
        {
            'name': 'field_o',
            'sql_line': 'field_o text NOT null,',
            'sql_type': 'text',
            'sql_width': None,
            'avro_type': 'string',
            'avro_meta': {},
            'nullable': False
        },
        {
            'name': 'field_p',
            'sql_line': 'field_p date NOT null,',
            'sql_type': 'date',
            'sql_width': None,
            'avro_type': 'string',
            'avro_meta': {'date': True},
            'nullable': False
        },
        {
            'name': 'field_q',
            'sql_line': '  field_q    timestamp NOT      null,',
            'sql_type': 'timestamp',
            'sql_width': None,
            'avro_type': 'long',
            'avro_meta': {'timestamp': True},
            'nullable': False
        },
        {
            'name': 'field_r',
            'sql_line': ' field_r    decimal ( 10,  5 ) not NULL  ,',
            'sql_type': 'decimal',
            'sql_width': [10, 5],
            'avro_type': 'double',
            'avro_meta': {
                'fixed_pt': True,
                'precision': 10,
                'scale': 5
            },
            'nullable': False
        },
        {
            'name': 'field_s',
            'sql_line': '    field_s    boolean   ,   ',
            'sql_type': 'boolean',
            'sql_width': None,
            'avro_type': ['boolean', 'null'],
            'avro_meta': {},
            'nullable': True
        },
    ]
    fixtures = copy.deepcopy(fixtures)
    for index, name in enumerate(pkeys):
        pkey_num = index + 1
        for fixture in fixtures:
            if fixture['name'] == name:
                fixture['avro_meta']['pkey'] = pkey_num
    for fixture in fixtures:
        fixture['avro_field'] = {
            'name': fixture['name'],
            'type': fixture['avro_type'],
            'doc': ''
        }
        fixture['avro_field'].update(fixture['avro_meta'])
    return fixtures


@pytest.fixture
def field_names(field_fixtures):
    return [field['name'] for field in field_fixtures]


@pytest.fixture
def avro_fields(field_fixtures):
    return [field['avro_field'] for field in field_fixtures]


@pytest.fixture
def sql_types(field_fixtures):
    return [field['sql_type'] for field in field_fixtures]


@pytest.fixture
def avro_types(field_fixtures):
    return [field['avro_type'] for field in field_fixtures]


@pytest.fixture
def sql_widths(field_fixtures):
    return [field['sql_width'] for field in field_fixtures]


@pytest.fixture
def nullables(field_fixtures):
    return [field['nullable'] for field in field_fixtures]


@pytest.fixture
def avro_metas(field_fixtures):
    return [field['avro_meta'] for field in field_fixtures]


@pytest.fixture
def field_sql_lines(field_fixtures):
    return [field['sql_line'] for field in field_fixtures]


class TestRedshiftFieldLineToAvroFieldConverter(object):

    @pytest.fixture
    def field_converters(self, field_sql_lines, pkeys):
        return [
            RedshiftFieldLineToAvroFieldConverter(
                field_line=line,
                pkeys=pkeys
            )
            for line in field_sql_lines
        ]

    def test_field_name(self, field_names, field_converters):
        for field_name, converter in zip(field_names, field_converters):
            assert converter.name == field_name

    def test_field_sql_type(self, sql_types, field_converters):
        for sql_type, converter in zip(sql_types, field_converters):
            assert converter.sql_type == sql_type

    def test_field_sql_type_width(self, sql_widths, field_converters):
        for width, converter in zip(sql_widths, field_converters):
            assert converter.sql_type_width == width

    def test_field_nullable(self, nullables, field_converters):
        for nullable, converter in zip(nullables, field_converters):
            assert converter.nullable == nullable

    def test_get_field_avro_type(self, avro_types, field_converters):
        for avro_type, converter in zip(avro_types, field_converters):
            assert converter.avro_type == avro_type

    def test_get_field_avro_meta_attr(self, avro_metas, field_converters):
        for meta_attr, converter in zip(avro_metas, field_converters):
            assert converter.avro_meta_attributes == meta_attr

    def test_get_avro_field(self, avro_fields, field_converters):
        for avro_field, converter in zip(avro_fields, field_converters):
            assert converter.avro_field == avro_field


class TestRedshiftSQLToAVSCConverter(object):

    @pytest.fixture()
    def default_schema(self):
        return 'public'

    @pytest.fixture(
        params=[
            {'table': 'test_table1'},
            {'schema': 'test_schema', 'table': 'test_table2'}]
    )
    def schema_table(self, request):
        return request.param

    @pytest.fixture
    def table(self, schema_table):
        return schema_table['table']

    @pytest.fixture
    def schema(self, schema_table, default_schema):
        return schema_table.get('schema', default_schema)

    @pytest.fixture(
        params=[
            'CREATE {schema_table}',
            'CREATE TABLE {schema_table}',
            '     CREATE     TABLE     {schema_table}     '
        ]
    )
    def create_table_line(self, request, schema_table):
        template = request.param
        schema = schema_table.get('schema')
        table = schema_table['table']
        if schema:
            return template.format(
                schema_table='{0}.{1}'.format(schema, table)
            )
        else:
            return template.format(
                schema_table=table
            )

    @pytest.fixture(
        params=[
            'PRIMARY KEY({keys})',
            '    PRIMARY      KEY     (   {keys}  )   '
        ]
    )
    def primary_keys_line(self, request, pkeys):
        template = request.param
        if pkeys:
            return template.format(keys=', '.join(pkeys))
        else:
            return ''

    @pytest.fixture
    def base_namespace(self):
        return 'test_base_namespace'

    @pytest.fixture(params=[
        ''');''',
        '''  ) distkey(field_a) sortkey(field_b);''',
        ''')
        DISTKEY(field_a)
        SORTKEY(field_b)
        COMMIT;''',
    ])
    def table_ending(self, request):
        return request.param

    @pytest.fixture(params=[
        '''
            {create_table}
            (
                {field_lines}
                {primary_key}
            {table_ending}
        ''',
        '''
            {create_table} (
                {field_lines}
                {primary_key}
            {table_ending}
        ''',
        '''
            {create_table}(
                {field_lines}
                {primary_key}
            {table_ending}
        '''
    ])
    def sql_content(
            self,
            request,
            table_ending,
            create_table_line,
            primary_keys_line,
            field_sql_lines
    ):
        template = request.param
        return template.format(
            create_table=create_table_line,
            field_lines='\n'.join(field_sql_lines),
            primary_key=primary_keys_line,
            table_ending=table_ending
        )

    @pytest.fixture
    def converter(self, sql_content, base_namespace, default_schema):
        return RedshiftSQLToAVSCConverter(
            sql_content=sql_content,
            base_namespace=base_namespace,
            default_schema=default_schema
        )

    @pytest.fixture
    def avro_record(self, namespace, table, pkeys, avro_fields):
        return {
            'type': 'record',
            'namespace': namespace,
            'name': table,
            'doc': '',
            'pkey': pkeys,
            'fields': avro_fields
        }

    @pytest.fixture
    def namespace(self, base_namespace, schema):
        return '{0}.{1}'.format(base_namespace, schema)

    def test_schema(self, schema, converter):
        assert converter.schema == schema

    def test_table(self, table, converter):
        assert converter.table == table

    def test_get_namespace(self, namespace, converter):
        assert converter.namespace == namespace

    def test_pkeys(self, pkeys, converter):
        assert converter.pkeys == pkeys

    def test_raw_field_lines(self, field_sql_lines, converter):
        assert converter._raw_field_lines == [
            _sanitize_line(line) for line in field_sql_lines
        ]

    def test_get_avro(self, avro_record, converter):
        assert converter.avro_record == avro_record
