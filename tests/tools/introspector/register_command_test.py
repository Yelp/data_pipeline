# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple
from uuid import uuid4

import mock
import pytest
import simplejson

from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.introspector.register_command import RegisterCommand


class FakeParserError(Exception):
    pass

Args = namedtuple(
    "Namespace", [
        'mysql_create_table',
        'mysql_old_create_table',
        'mysql_alter_table',
        'avro_schema',
        'base_schema_id',
        'source_owner_email',
        'pii',
        'source',
        'namespace',
        'verbosity'
    ]
)


class TestInfoCommand(object):

    @pytest.fixture
    def register_command(self, containers):
        register_command = RegisterCommand("data_pipeline_introspector_schema_check")
        register_command.log = mock.Mock()
        register_command.log.info = mock.Mock()
        register_command.log.debug = mock.Mock()
        register_command.log.warning = mock.Mock()
        register_command.print_schema_dict = mock.Mock()
        return register_command

    @pytest.fixture
    def parser(self):
        parser = mock.Mock()
        parser.error = FakeParserError
        return parser

    @pytest.fixture
    def schematizer(self):
        return get_schematizer()

    @pytest.fixture
    def namespace_name(self):
        return "schema_check_namespace_{0}".format(uuid4())

    @pytest.fixture
    def source_name(self):
        return "schema_check_source_{0}".format(uuid4())

    @pytest.fixture
    def source_owner_email(self):
        return "bam+test@yelp.com"

    @pytest.fixture
    def schema_json(self, namespace_name, source_name):
        return {
            'type': 'record',
            'name': source_name,
            'namespace': namespace_name,
            'fields': [{'type': 'int', 'name': 'biz_id'}]
        }

    @pytest.fixture
    def schema_str(self, schema_json):
        return simplejson.dumps(schema_json)

    @pytest.fixture
    def old_create_biz_table_stmt(self, source_name):
        return 'create table {}(id int(11) not null);'.format(
            source_name
        )

    @pytest.fixture
    def alter_biz_table_stmt(self, source_name):
        return 'alter table {} add column name varchar(8);'.format(
            source_name
        )

    @pytest.fixture
    def new_create_biz_table_stmt(self, source_name):
        return 'create table {}(id int(11) not null, name varchar(8));'.format(
            source_name
        )

    @pytest.fixture
    def avro_schema_of_new_biz_table(self, source_name):
        return {
            'type': 'record',
            'name': source_name.split('-')[0],
            'namespace': '',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'name',
                 'type': ['null', 'string'], 'maxlen': '8', 'default': None}
            ]
        }

    def _create_fake_args(
        self,
        source,
        source_owner_email,
        namespace=None,
        avro_schema=None,
        mysql_create_table=None,
        mysql_old_create_table=None,
        mysql_alter_table=None,
        base_schema_id=None,
        pii=False
    ):
        return Args(
            source=source,
            source_owner_email=source_owner_email,
            namespace=namespace,
            avro_schema=avro_schema,
            mysql_create_table=mysql_create_table,
            mysql_old_create_table=mysql_old_create_table,
            mysql_alter_table=mysql_alter_table,
            base_schema_id=base_schema_id,
            pii=pii,
            verbosity=0
        )

    def test_no_schemas(
        self,
        register_command,
        parser,
        source_name,
        source_owner_email,
        namespace_name
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            avro_schema=None
        )
        with pytest.raises(FakeParserError) as e:
            register_command.run(args, parser)
        assert e.value.args
        assert "--avro-schema or --mysql-create-table is required" == e.value.args[0]

    def test_avro_schema(
        self,
        register_command,
        parser,
        source_name,
        source_owner_email,
        namespace_name,
        schema_str,
        schema_json
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            avro_schema=schema_str
        )
        register_command.run(args, parser)
        assert register_command.print_schema_dict.call_count == 1
        call_args, _ = register_command.print_schema_dict.call_args
        schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=schema_dict,
            primary_keys=[],
            schema_json=schema_json,
            namespace_name=namespace_name,
            source_name=source_name,
        )

    def test_avro_schema_with_warnings(
        self,
        register_command,
        parser,
        source_name,
        source_owner_email,
        namespace_name,
        schema_str,
        schema_json,
        new_create_biz_table_stmt,
        old_create_biz_table_stmt,
        alter_biz_table_stmt
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            avro_schema=schema_str,
            mysql_create_table=new_create_biz_table_stmt,
            mysql_old_create_table=old_create_biz_table_stmt,
            mysql_alter_table=alter_biz_table_stmt
        )
        register_command.run(args, parser)

        mysql_exclusive_fields = [
            'mysql_create_table', 'mysql_old_create_table', 'mysql_alter_table'
        ]
        register_command.log.warning.assert_called_once_with(
            "Given fields: {} will not be used, since --avro_schema was given".format(
                mysql_exclusive_fields
            )
        )

        assert register_command.print_schema_dict.call_count == 1
        call_args, _ = register_command.print_schema_dict.call_args
        schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=schema_dict,
            primary_keys=[],
            schema_json=schema_json,
            namespace_name=namespace_name,
            source_name=source_name,
        )

    def test_avro_schema_base_id_and_pii(
        self,
        register_command,
        parser,
        source_name,
        source_owner_email,
        namespace_name,
        schema_str,
        schema_json
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            avro_schema=schema_str,
            base_schema_id=1,
            pii=True
        )
        register_command.run(args, parser)
        assert register_command.print_schema_dict.call_count == 1
        call_args, _ = register_command.print_schema_dict.call_args
        schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=schema_dict,
            primary_keys=[],
            schema_json=schema_json,
            namespace_name=namespace_name,
            source_name=source_name,
            base_schema_id=1,
            contains_pii=True
        )

    def test_avro_schema_with_no_namespace(
        self,
        register_command,
        parser,
        source_name,
        source_owner_email,
        schema_str
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=None,
            avro_schema=schema_str
        )
        with pytest.raises(FakeParserError) as e:
            register_command.run(args, parser)
        assert e.value.args
        assert "--namespace must be provided" in e.value.args[0]

    def test_mysql_create_table(
        self,
        register_command,
        parser,
        namespace_name,
        source_name,
        source_owner_email,
        new_create_biz_table_stmt,
        avro_schema_of_new_biz_table
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            mysql_create_table=new_create_biz_table_stmt
        )
        register_command.run(args, parser)
        assert register_command.print_schema_dict.call_count == 1
        call_args, _ = register_command.print_schema_dict.call_args
        schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=schema_dict,
            primary_keys=[],
            schema_json=avro_schema_of_new_biz_table,
            namespace_name=namespace_name,
            source_name=source_name
        )

    def test_mysql_update_existing_table(
        self,
        register_command,
        parser,
        namespace_name,
        source_name,
        source_owner_email,
        new_create_biz_table_stmt,
        alter_biz_table_stmt,
        old_create_biz_table_stmt,
        avro_schema_of_new_biz_table
    ):
        args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            mysql_create_table=new_create_biz_table_stmt,
            mysql_old_create_table=old_create_biz_table_stmt,
            mysql_alter_table=alter_biz_table_stmt
        )
        register_command.run(args, parser)
        assert register_command.print_schema_dict.call_count == 1
        call_args, _ = register_command.print_schema_dict.call_args
        schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=schema_dict,
            primary_keys=[],
            schema_json=avro_schema_of_new_biz_table,
            namespace_name=namespace_name,
            source_name=source_name
        )

    def test_mysql_same_schema_diff_pii(
        self,
        register_command,
        parser,
        namespace_name,
        source_name,
        source_owner_email,
        new_create_biz_table_stmt,
        avro_schema_of_new_biz_table
    ):
        non_pii_args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            mysql_create_table=new_create_biz_table_stmt
        )
        pii_args = self._create_fake_args(
            source=source_name,
            source_owner_email=source_owner_email,
            namespace=namespace_name,
            mysql_create_table=new_create_biz_table_stmt,
            pii=True
        )
        register_command.run(non_pii_args, parser)
        assert register_command.print_schema_dict.call_count == 1
        call_args, _ = register_command.print_schema_dict.call_args
        non_pii_schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=non_pii_schema_dict,
            primary_keys=[],
            schema_json=avro_schema_of_new_biz_table,
            namespace_name=namespace_name,
            source_name=source_name
        )

        register_command.run(pii_args, parser)
        assert register_command.print_schema_dict.call_count == 2
        call_args, _ = register_command.print_schema_dict.call_args
        pii_schema_dict = call_args[0]
        self._assert_correct_schema_dict(
            schema_dict=pii_schema_dict,
            primary_keys=[],
            schema_json=avro_schema_of_new_biz_table,
            namespace_name=namespace_name,
            source_name=source_name,
            contains_pii=True
        )

        assert non_pii_schema_dict['topic']['topic_id'] != pii_schema_dict['topic']['topic_id']

    def _assert_correct_schema_dict(
        self,
        schema_dict,
        primary_keys,
        schema_json,
        namespace_name,
        source_name,
        contains_pii=False,
        base_schema_id=None
    ):
        assert schema_dict['primary_keys'] == primary_keys
        assert schema_dict['schema_json'] == schema_json
        assert schema_dict['base_schema_id'] == base_schema_id
        assert schema_dict['topic']['source_name'] == source_name
        assert schema_dict['topic']['namespace'] == namespace_name
        assert schema_dict['topic']['contains_pii'] == contains_pii
