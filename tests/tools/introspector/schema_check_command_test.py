# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple
from uuid import uuid4

import mock
import pytest
import simplejson

from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.introspector.schema_check_command import SchemaCheckCommand


class FakeParserError(Exception):
    pass

Args = namedtuple(
    "Namespace", [
        "schema",
        "source",
        "namespace",
        "verbosity"
    ]
)


class TestInfoCommand(object):

    @pytest.fixture
    def schema_check_command(self):
        schema_check_command = SchemaCheckCommand("data_pipeline_introspector_schema_check")
        schema_check_command.log = mock.Mock()
        schema_check_command.log.info = mock.Mock()
        schema_check_command.log.debug = mock.Mock()
        schema_check_command.log.warning = mock.Mock()
        return schema_check_command

    @pytest.fixture
    def parser(self):
        parser = mock.Mock()
        parser.error = FakeParserError
        return parser

    @pytest.fixture(scope='class')
    def schematizer(self, containers):
        return get_schematizer()

    @pytest.fixture(scope='class')
    def namespace_name(self):
        return "schema_check_namespace_{0}".format(uuid4())

    @pytest.fixture(scope='class')
    def source_name(self):
        return "schema_check_source_{0}".format(uuid4())

    @pytest.fixture(scope='class')
    def schema_json(self, namespace_name, source_name):
        return {
            'type': 'record',
            'name': source_name,
            'namespace': namespace_name,
            'fields': [{'type': 'int', 'name': 'biz_id'}]
        }

    @pytest.fixture(scope='class')
    def schema_json_incompatible(self, namespace_name, source_name):
        return {
            'type': 'record',
            'name': source_name,
            'namespace': namespace_name,
            'fields': [
                {'type': 'int', 'name': 'biz_id'},
                {'type': 'int', 'name': 'new_field'}
            ]
        }

    @pytest.fixture(scope='class')
    def schema_str(self, schema_json):
        return simplejson.dumps(schema_json)

    @pytest.fixture(scope='class')
    def schema_str_incompatible(self, schema_json_incompatible):
        return simplejson.dumps(schema_json_incompatible)

    @pytest.fixture(autouse=True, scope='class')
    def schema(self, schematizer, namespace_name, source_name, schema_str):
        return schematizer.register_schema(
            namespace=namespace_name,
            source=source_name,
            schema_str=schema_str,
            source_owner_email="bam+test@yelp.com",
            contains_pii=False
        )

    @pytest.fixture
    def source_id(self, schema):
        return str(schema.topic.source.source_id)

    @pytest.fixture(params=['names', 'id'])
    def compatible_args(
        self,
        request,
        source_name,
        namespace_name,
        source_id,
        schema_str
    ):
        return {
            'names': self._create_fake_args(
                schema=schema_str,
                source=source_name,
                namespace=namespace_name
            ),
            'id': self._create_fake_args(
                schema=schema_str,
                source=source_id
            )
        }[request.param]

    @pytest.fixture(params=['names', 'id'])
    def incompatible_args(
        self,
        request,
        source_name,
        namespace_name,
        source_id,
        schema_str_incompatible
    ):
        return {
            'names': self._create_fake_args(
                schema=schema_str_incompatible,
                source=source_name,
                namespace=namespace_name
            ),
            'id': self._create_fake_args(
                schema=schema_str_incompatible,
                source=source_id
            )
        }[request.param]

    def _create_fake_args(
        self,
        schema,
        source,
        namespace=None
    ):
        return Args(
            schema=schema,
            source=source,
            namespace=namespace,
            verbosity=0
        )

    def _test_run_command(
        self,
        schema_check_command,
        args,
        parser
    ):
        schema_check_command.process_args(args, parser)
        return schema_check_command.is_compatible()

    def test_compatible(
        self,
        schema_check_command,
        compatible_args,
        parser
    ):
        assert self._test_run_command(
            schema_check_command,
            compatible_args,
            parser
        )

    def test_incompatible(
        self,
        schema_check_command,
        incompatible_args,
        parser
    ):
        assert not self._test_run_command(
            schema_check_command,
            incompatible_args,
            parser
        )

    def test_missing_name_error(
        self,
        schema_check_command,
        parser,
        source_name
    ):
        with pytest.raises(FakeParserError) as e:
            schema_check_command.process_args(
                self._create_fake_args(
                    schema="{}",
                    source=source_name
                ),
                parser
            )
        assert e.value.args
        assert "--namespace must be provided when given a source name as source identifier" in e.value.args[0]

    def test_unneeded_namespace_warning(
        self,
        schema_check_command,
        parser,
        source_id,
        namespace_name
    ):
        schema_check_command.process_args(
            self._create_fake_args(
                schema="{}",
                source=source_id,
                namespace=namespace_name
            ),
            parser
        )
        schema_check_command.log.warning.assert_called_once_with(
            "Since source id was given, --namespace will be ignored"
        )
