# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple
from uuid import uuid4

import mock
import pytest
import simplejson

from data_pipeline.tools.introspector.register.avro_command import RegisterAvroCommand
from data_pipeline.tools.introspector.register.mysql_command import RegisterMysqlCommand
from tests.tools.introspector.base_test import FakeParserError
from tests.tools.introspector.base_test import TestIntrospectorBase

AvroArgs = namedtuple(
    "Namespace", [
        "source_id",
        "source_name",
        "namespace",
        "source_owner_email",
        "pii",
        "cluster_type",
        "avro_schema",
        "base_schema_id",
        "verbosity"
    ]
)

MysqlArgs = namedtuple(
    "Namespace", [
        "source_id",
        "source_name",
        "namespace",
        "source_owner_email",
        "pii",
        "create_table",
        "old_create_table",
        "alter_table",
        "verbosity"
    ]
)


class BaseTestRegister(TestIntrospectorBase):
    def _assert_correct_schema(
        self,
        schema,
        primary_keys,
        schema_json,
        namespace_name,
        source_name,
        contains_pii=False,
        cluster_type='datapipe',
        base_schema_id=None
    ):
        assert schema.primary_keys == primary_keys
        assert schema.schema_json == schema_json
        assert schema.base_schema_id == base_schema_id
        assert schema.topic.source.name == source_name
        assert schema.topic.source.namespace.name == namespace_name
        assert schema.topic.contains_pii == contains_pii
        assert schema.topic.cluster_type == cluster_type

    @pytest.fixture
    def namespace_name(self):
        return "schema_check_namespace_{0}".format(uuid4())

    @pytest.fixture
    def source_name(self):
        return "schema_check_source_{0}".format(uuid4())


class TestRegisterAvroCommand(BaseTestRegister):

    @pytest.fixture
    def register_command(self, containers):
        register_command = RegisterAvroCommand("data_pipeline_introspector_register_avro")
        register_command.log = mock.Mock()
        register_command.log.info = mock.Mock()
        register_command.log.debug = mock.Mock()
        register_command.log.warning = mock.Mock()
        register_command.print_schema = mock.Mock()
        return register_command

    @pytest.fixture
    def schema_json(self, namespace_name, source_name):
        return {
            'type': 'record',
            'name': source_name,
            'namespace': namespace_name,
            'doc': 'test',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'biz_id'}]
        }

    @pytest.fixture
    def schema_str(self, schema_json):
        return simplejson.dumps(schema_json)

    def _create_fake_args(
        self,
        source_id=None,
        source_name=None,
        namespace=None,
        avro_schema=None,
        base_schema_id=None,
        pii=False,
        cluster_type=None
    ):
        return AvroArgs(
            source_id=source_id,
            source_name=source_name,
            source_owner_email=self.source_owner_email,
            namespace=namespace,
            avro_schema=avro_schema,
            base_schema_id=base_schema_id,
            pii=pii,
            cluster_type=cluster_type,
            verbosity=0
        )

    @pytest.mark.parametrize("overrides, expected_overrides", [
        ({}, {}),
        ({'base_schema_id': 1}, {'base_schema_id': 1}),
        ({'pii': True}, {'contains_pii': True}),
        ({'cluster_type': 'scribe'}, {'cluster_type': 'scribe'})
    ])
    def test_avro_schema(
        self,
        register_command,
        parser,
        source_name,
        namespace_name,
        schema_str,
        schema_json,
        overrides,
        expected_overrides
    ):
        args = self._create_fake_args(
            source_name=source_name,
            namespace=namespace_name,
            avro_schema=schema_str,
            **overrides
        )
        register_command.run(args, parser)
        assert register_command.print_schema.call_count == 1
        call_args, _ = register_command.print_schema.call_args
        schema = call_args[0]
        self._assert_correct_schema(
            schema=schema,
            primary_keys=[],
            schema_json=schema_json,
            namespace_name=namespace_name,
            source_name=source_name,
            **expected_overrides
        )

    def test_avro_schema_with_no_namespace(
        self,
        register_command,
        parser,
        source_name,
        schema_str
    ):
        args = self._create_fake_args(
            source_name=source_name,
            namespace=None,
            avro_schema=schema_str
        )
        with pytest.raises(FakeParserError) as e:
            register_command.run(args, parser)
        assert e.value.args
        assert "--namespace must be provided" in e.value.args[0]


class TestRegisterMysqlCommand(BaseTestRegister):

    @pytest.fixture
    def register_command(self, containers):
        register_command = RegisterMysqlCommand("data_pipeline_introspector_register_mysql")
        register_command.log = mock.Mock()
        register_command.log.info = mock.Mock()
        register_command.log.debug = mock.Mock()
        register_command.log.warning = mock.Mock()
        register_command.print_schema = mock.Mock()
        return register_command

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
            'doc': 'test',
            'fields': [
                {'name': 'id', 'doc': 'test', 'type': 'int'},
                {'name': 'name', 'doc': 'test',
                 'type': ['null', 'string'], 'maxlen': 8, 'default': None}
            ]
        }

    def _create_fake_args(
        self,
        source_id=None,
        source_name=None,
        namespace=None,
        create_table=None,
        old_create_table=None,
        alter_table=None,
        pii=False
    ):
        return MysqlArgs(
            source_id=source_id,
            source_name=source_name,
            source_owner_email=self.source_owner_email,
            namespace=namespace,
            create_table=create_table,
            old_create_table=old_create_table,
            alter_table=alter_table,
            pii=pii,
            verbosity=0
        )

    def test_create_table(
        self,
        register_command,
        parser,
        namespace_name,
        source_name,
        new_create_biz_table_stmt,
        avro_schema_of_new_biz_table
    ):
        args = self._create_fake_args(
            source_name=source_name,
            namespace=namespace_name,
            create_table=new_create_biz_table_stmt
        )
        register_command.run(args, parser)
        assert register_command.print_schema.call_count == 1
        call_args, _ = register_command.print_schema.call_args
        schema = call_args[0]
        schema.schema_json['doc'] = 'test'
        schema.schema_json['fields'][0]['doc'] = 'test'
        schema.schema_json['fields'][1]['doc'] = 'test'
        self._assert_correct_schema(
            schema=schema,
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
        new_create_biz_table_stmt,
        alter_biz_table_stmt,
        old_create_biz_table_stmt,
        avro_schema_of_new_biz_table
    ):
        args = self._create_fake_args(
            source_name=source_name,
            namespace=namespace_name,
            create_table=new_create_biz_table_stmt,
            old_create_table=old_create_biz_table_stmt,
            alter_table=alter_biz_table_stmt
        )
        register_command.run(args, parser)
        assert register_command.print_schema.call_count == 1
        call_args, _ = register_command.print_schema.call_args
        schema = call_args[0]
        schema.schema_json['doc'] = 'test'
        schema.schema_json['fields'][0]['doc'] = 'test'
        schema.schema_json['fields'][1]['doc'] = 'test'
        self._assert_correct_schema(
            schema=schema,
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
        new_create_biz_table_stmt,
        avro_schema_of_new_biz_table
    ):
        non_pii_args = self._create_fake_args(
            source_name=source_name,
            namespace=namespace_name,
            create_table=new_create_biz_table_stmt
        )
        pii_args = self._create_fake_args(
            source_name=source_name,
            namespace=namespace_name,
            create_table=new_create_biz_table_stmt,
            pii=True
        )
        register_command.run(non_pii_args, parser)
        assert register_command.print_schema.call_count == 1
        call_args, _ = register_command.print_schema.call_args
        non_pii_schema = call_args[0]
        non_pii_schema.schema_json['doc'] = 'test'
        non_pii_schema.schema_json['fields'][0]['doc'] = 'test'
        non_pii_schema.schema_json['fields'][1]['doc'] = 'test'
        self._assert_correct_schema(
            schema=non_pii_schema,
            primary_keys=[],
            schema_json=avro_schema_of_new_biz_table,
            namespace_name=namespace_name,
            source_name=source_name
        )

        register_command.run(pii_args, parser)
        assert register_command.print_schema.call_count == 2
        call_args, _ = register_command.print_schema.call_args
        pii_schema = call_args[0]
        pii_schema.schema_json['doc'] = 'test'
        pii_schema.schema_json['fields'][0]['doc'] = 'test'
        pii_schema.schema_json['fields'][1]['doc'] = 'test'
        self._assert_correct_schema(
            schema=pii_schema,
            primary_keys=[],
            schema_json=avro_schema_of_new_biz_table,
            namespace_name=namespace_name,
            source_name=source_name,
            contains_pii=True
        )

        assert non_pii_schema.topic.topic_id != pii_schema.topic.topic_id
