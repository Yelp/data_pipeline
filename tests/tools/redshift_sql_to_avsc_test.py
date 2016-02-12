# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline.tools.redshift_sql_to_avsc import _sanitize_line
from data_pipeline.tools.redshift_sql_to_avsc \
    import RedshiftFieldLineToAvroFieldConverter
from data_pipeline.tools.redshift_sql_to_avsc import RedshiftSQLToAVSCBatch
from data_pipeline.tools.redshift_sql_to_avsc \
    import RedshiftSQLToAVSCConverter


@pytest.fixture(
    # in case more primary keys are adding in future please update the
    # 'sql_line' in fixtures with {primary_key} formatter as shown in fixture field_a, field_b, field_c
    params=[
        [],
        ['field_a'],
        ['field_a', 'field_b'],
        ['field_b', 'field_a', 'field_c'],
    ]
)
def pkeys(request):
    return request.param


@pytest.fixture
def field_fixtures(pkeys):
    fixtures = [
        {
            'name': 'field_a',
            'sql_line': "field_a char(18) {primary_key}  NOT NULL default '' encode lzo,",
            'sql_type': 'char',
            'sql_width': 18,
            'avro_type': 'string',
            'avro_meta': {'fixlen': 18, 'default': ''},
            'nullable': False,
            'sql_default': ''
        },
        {
            'name': 'field_b',
            'sql_line': ' field_b  varchar ( 320 ) {primary_key}  default NULL encode lzo , ',
            'sql_type': 'varchar',
            'sql_width': 320,
            'avro_type': ['null', 'string'],
            'avro_meta': {'maxlen': 320, 'default': None},
            'nullable': True,
            'sql_default': 'null'
        },
        {
            'name': 'field_c',
            'sql_line': 'field_c integer {primary_key}  DEFAULT NULL,',
            'sql_type': 'integer',
            'sql_width': None,
            'avro_type': ['null', 'int'],
            'avro_meta': {'default': None},
            'nullable': True,
            'sql_default': 'null'
        },
        {
            'name': 'field_d',
            'sql_line': 'field_d int   NOT    NULL  default "1",',
            'sql_type': 'int',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {'default': 1},
            'nullable': False,
            'sql_default': '1'
        },
        {
            'name': 'field_e',
            'sql_line': 'field_e int4 NOT NULL,',
            'sql_type': 'int4',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {},
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_f',
            'sql_line': 'field_f SMALLINT NOT NULL  DEFAULT 42 ,',
            'sql_type': 'smallint',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {'default': 42},
            'nullable': False,
            'sql_default': '42'
        },
        {
            'name': 'field_g',
            'sql_line': 'field_g int2 NOT NULL,',
            'sql_type': 'int2',
            'sql_width': None,
            'avro_type': 'int',
            'avro_meta': {},
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_h',
            'sql_line': 'field_h bigint,',
            'sql_type': 'bigint',
            'sql_width': None,
            'avro_type': ['null', 'long'],
            'avro_meta': {'default': None},
            'nullable': True,
            'sql_default': None
        },
        {
            'name': 'field_i',
            'sql_line': 'field_i INT8,',
            'sql_type': 'int8',
            'sql_width': None,
            'avro_type': ['null', 'long'],
            'avro_meta': {'default': None},
            'nullable': True,
            'sql_default': None
        },
        {
            'name': 'field_j',
            'sql_line': 'field_j real NULL,',
            'sql_type': 'real',
            'sql_width': None,
            'avro_type': ['null', 'float'],
            'avro_meta': {'default': None},
            'nullable': True,
            'sql_default': None
        },
        {
            'name': 'field_k',
            'sql_line': '   field_k    float4    not    null   ,   ',
            'sql_type': 'float4',
            'sql_width': None,
            'avro_type': 'float',
            'avro_meta': {},
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_l',
            'sql_line': 'field_l double not null,',
            'sql_type': 'double',
            'sql_width': None,
            'avro_type': 'double',
            'avro_meta': {},
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_m',
            'sql_line': 'field_m   float8      null   default "10.9",',
            'sql_type': 'float8',
            'sql_width': None,
            'avro_type': ['double', 'null'],
            'avro_meta': {'default': 10.9},
            'nullable': True,
            'sql_default': '10.9'
        },
        {
            'name': 'field_n',
            'sql_line': '   field_n FLOAT default 4.2,',
            'sql_type': 'float',
            'sql_width': None,
            'avro_type': ['double', 'null'],
            'avro_meta': {'default': 4.2},
            'nullable': True,
            'sql_default': '4.2'
        },
        {
            'name': 'field_o',
            'sql_line': 'field_o text NOT null,',
            'sql_type': 'text',
            'sql_width': 256,
            'avro_type': 'string',
            'avro_meta': {'maxlen': 256},
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_p',
            'sql_line': 'field_p date NOT null,',
            'sql_type': 'date',
            'sql_width': None,
            'avro_type': 'string',
            'avro_meta': {'date': True},
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_q',
            'sql_line': '  field_q    timestamp NOT      null,',
            'sql_type': 'timestamp',
            'sql_width': None,
            'avro_type': 'long',
            'avro_meta': {'timestamp': True},
            'nullable': False,
            'sql_default': None
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
            'nullable': False,
            'sql_default': None
        },
        {
            'name': 'field_s',
            'sql_line': '    field_s    boolean  DEFAULT false  ,   ',
            'sql_type': 'boolean',
            'sql_width': None,
            'avro_type': ['boolean', 'null'],
            'avro_meta': {'default': False},
            'nullable': True,
            'sql_default': 'false'
        },
        {
            'name': 'field_s1',
            'sql_line': '    field_s1    boolean  DEFAULT 0 ,   ',
            'sql_type': 'boolean',
            'sql_width': None,
            'avro_type': ['boolean', 'null'],
            'avro_meta': {'default': False},
            'nullable': True,
            'sql_default': '0'
        },
        {
            'name': 'field_s2',
            'sql_line': 'field_s2 boolean default true,',
            'sql_type': 'boolean',
            'sql_width': None,
            'avro_type': ['boolean', 'null'],
            'avro_meta': {'default': True},
            'nullable': True,
            'sql_default': 'true'
        },
        {
            'name': 'field_s3',
            'sql_line': 'field_s3 boolean default 1,',
            'sql_type': 'boolean',
            'sql_width': None,
            'avro_type': ['boolean', 'null'],
            'avro_meta': {'default': True},
            'nullable': True,
            'sql_default': '1'
        },
        {
            'name': 'field_t',
            'sql_line': 'field_t character(12),',
            'sql_type': 'character',
            'sql_width': 12,
            'avro_type': ['null', 'string'],
            'avro_meta': {'default': None, 'fixlen': 12},
            'nullable': True,
            'sql_default': None
        },
        {
            'name': 'field_u',
            'sql_line': 'field_u bpchar,',
            'sql_type': 'bpchar',
            'sql_width': 256,
            'avro_type': ['null', 'string'],
            'avro_meta': {'default': None, 'fixlen': 256},
            'nullable': True,
            'sql_default': None
        },
        {
            'name': 'field_v',
            'sql_line': 'field_v NCHAR(111) default "test",',
            'sql_type': 'nchar',
            'sql_width': 111,
            'avro_type': ['string', 'null'],
            'avro_meta': {'fixlen': 111, 'default': 'test'},
            'nullable': True,
            'sql_default': 'test'
        },
        {
            'name': 'field_w',
            'sql_line': "field_w NVARCHAR(42) NOT NULL default ' test ' , ",
            'sql_type': 'nvarchar',
            'sql_width': 42,
            'avro_type': 'string',
            'avro_meta': {'maxlen': 42, 'default': ' test '},
            'nullable': False,
            'sql_default': ' test '
        },
        {
            'name': 'field_x',
            'sql_line': "field_x bool,",
            'sql_type': 'bool',
            'sql_width': None,
            'avro_type': ['null', 'boolean'],
            'avro_meta': {'default': None},
            'nullable': True,
            'sql_default': None
        },
        {
            'name': 'field_y',
            'sql_line': 'field_y numeric(10, 5),',
            'sql_type': 'numeric',
            'sql_width': [10, 5],
            'avro_type': ['null', 'double'],
            'avro_meta': {
                'default': None,
                'fixed_pt': True,
                'precision': 10,
                'scale': 5
            },
            'nullable': True,
            'sql_default': None
        },
    ]
    fixtures = fixtures
    pk_first_flag = False
    for index, name in enumerate(pkeys):
        pkey_num = index + 1
        for fixture in fixtures:
            if fixture['name'] == name:
                fixture['avro_meta']['pkey'] = pkey_num
                if not pk_first_flag:
                    # first pk in pkeys is mocked as coming from field line
                    # and others are coming from primary keys lines
                    pk_first_flag = True
                    fixture['sql_line'] = fixture['sql_line'].format(
                        primary_key='primary key'
                    )

    for fixture in fixtures:
        fixture['sql_line'] = fixture['sql_line'].format(
            primary_key=''
        )
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
def sql_defaults(field_fixtures):
    return [field['sql_default'] for field in field_fixtures]


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

    def test_name(self, field_names, field_converters):
        for field_name, converter in zip(field_names, field_converters):
            assert converter.name == field_name

    def test_sql_type(self, sql_types, field_converters):
        for sql_type, converter in zip(sql_types, field_converters):
            assert converter.sql_type == sql_type

    def test_sql_type_width(self, sql_widths, field_converters):
        for width, converter in zip(sql_widths, field_converters):
            assert converter.sql_type_width == width

    def test_nullable(self, nullables, field_converters):
        for nullable, converter in zip(nullables, field_converters):
            assert converter.nullable == nullable

    def test_avro_type(self, avro_types, field_converters):
        for avro_type, converter in zip(avro_types, field_converters):
            assert converter.avro_type == avro_type

    def test_avro_meta_attr(self, avro_metas, field_converters):
        for meta_attr, converter in zip(avro_metas, field_converters):
            assert converter.avro_meta_attributes == meta_attr

    def test_sql_default(self, sql_defaults, field_converters):
        for sql_default, converter in zip(sql_defaults, field_converters):
            assert converter.sql_default == sql_default

    def test_avro_field(self, avro_fields, field_converters):
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
        # this hack mocks the first key in pkeys as field line pk
        # and all others keys as coming from primary keys lines
        if pkeys and len(pkeys) > 1:
            return template.format(keys=', '.join(pkeys[1:]))
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


class TestRedshiftSQLToAVSCBatch(object):
    @pytest.fixture
    def globs(self):
        return ['test/*.sql']

    @pytest.fixture
    def sql_file_path(self):
        return 'test/test.sql'

    @pytest.fixture
    def avsc_file_path(self):
        return 'test/test.avsc'

    @pytest.yield_fixture
    def mock_get_file_paths_from_glob_patterns(self, sql_file_path):
        with mock.patch(
            'data_pipeline.tools.redshift_sql_to_avsc.get_file_paths_from_glob_patterns'
        ) as mock_get_file_paths_from_glob_patterns:
            mock_get_file_paths_from_glob_patterns.return_value = [sql_file_path]
            yield mock_get_file_paths_from_glob_patterns

    @pytest.yield_fixture
    def mock_os_path_exists(self):
        with mock.patch(
            'data_pipeline.tools.redshift_sql_to_avsc.os.path.exists'
        ) as mock_os_path_exists:
            mock_os_path_exists.return_value = True
            yield mock_os_path_exists

    @pytest.fixture
    def mock_batch(self, globs):
        batch = RedshiftSQLToAVSCBatch()
        batch.convert_sql_to_avsc = mock.Mock()
        batch.options = mock.Mock()
        batch.options.globs = globs
        return batch

    def test_run_skips_existing_if_overwrite_false(
            self,
            avsc_file_path,
            globs,
            mock_get_file_paths_from_glob_patterns,
            mock_os_path_exists,
            mock_batch
    ):
        mock_batch.options.overwrite = False
        mock_batch.run()
        assert mock_get_file_paths_from_glob_patterns.mock_calls == [
            mock.call(glob_patterns=globs)
        ]
        assert mock_os_path_exists.mock_calls == [mock.call(avsc_file_path)]
        assert mock_batch.convert_sql_to_avsc.mock_calls == []

    def test_run_converts_existing_if_overwrite_true(
            self,
            sql_file_path,
            avsc_file_path,
            globs,
            mock_get_file_paths_from_glob_patterns,
            mock_os_path_exists,
            mock_batch
    ):
        mock_batch.options.overwrite = True
        mock_batch.run()
        assert mock_get_file_paths_from_glob_patterns.mock_calls == [
            mock.call(glob_patterns=globs)
        ]
        assert mock_os_path_exists.mock_calls == [mock.call(avsc_file_path)]
        assert mock_batch.convert_sql_to_avsc.mock_calls == [
            mock.call(
                avsc_file_path=avsc_file_path,
                sql_file_path=sql_file_path
            )
        ]
