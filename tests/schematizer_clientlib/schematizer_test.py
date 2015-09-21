# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import simplejson

from data_pipeline.schematizer_clientlib.schematizer import SchematizerClient


class SchematizerClientTestBase(object):

    @pytest.fixture
    def schematizer(self):
        return SchematizerClient()

    def attach_spy_on_api(self, resource, api_name):
        original_func = getattr(resource, api_name)

        def attach_spy(*args, **kwargs):
            return original_func(*args, **kwargs)

        return mock.patch.object(resource, api_name, side_effect=attach_spy)


class TestSchematizerClient(SchematizerClientTestBase):

    def test_get_non_cached_schema_by_id(self, schematizer, registered_schema):
        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schema_by_id'
        ) as api_spy:
            actual = schematizer.get_schema_by_id(registered_schema.schema_id)
            self._assert_schema_values(actual, registered_schema)
            assert api_spy.call_count == 1

    def test_get_cached_schema_by_id(self, schematizer, registered_schema):
        schematizer.get_schema_by_id(registered_schema.schema_id)

        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schema_by_id'
        ) as schema_api_spy, self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_schema_by_id(registered_schema.schema_id)
            self._assert_schema_values(actual, registered_schema)
            assert schema_api_spy.call_count == 0
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0

    def test_get_non_cached_topic_by_name(self, schematizer, registered_schema):
        expected_topic = registered_schema.topic

        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as api_spy:
            actual = schematizer.get_topic_by_name(expected_topic.name)
            self._assert_topic_values(actual, expected_topic)
            assert api_spy.call_count == 1

    def test_get_cached_topic_by_name(self, schematizer, registered_schema):
        expected_topic = registered_schema.topic
        schematizer.get_topic_by_name(expected_topic.name)

        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_topic_by_name(expected_topic.name)
            self._assert_topic_values(actual, expected_topic)
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0

    def test_get_non_cached_source_by_id(self, schematizer, registered_schema):
        expected_source = registered_schema.topic.source

        with self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as api_spy:
            actual = schematizer.get_source_by_id(expected_source.source_id)
            self._assert_source_values(actual, expected_source)
            assert api_spy.call_count == 1

    def test_get_cached_source_by_id(self, schematizer, registered_schema):
        expected_source = registered_schema.topic.source
        schematizer.get_source_by_id(expected_source.source_id)

        with self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_source_by_id(expected_source.source_id)
            self._assert_source_values(actual, expected_source)
            assert source_api_spy.call_count == 0

    def _assert_schema_values(self, actual, expected):
        assert actual.schema_id == expected.schema_id
        assert actual.schema_json == simplejson.loads(expected.schema)
        self._assert_topic_values(actual.topic, expected.topic)
        assert actual.base_schema_id == expected.base_schema_id
        assert actual.created_at == expected.created_at
        assert actual.updated_at == expected.updated_at

    def _assert_topic_values(self, actual, expected):
        assert actual.topic_id == expected.topic_id
        assert actual.name == expected.name
        self._assert_source_values(actual.source, expected.source)
        assert actual.contains_pii == expected.contains_pii
        assert actual.created_at == expected.created_at
        assert actual.updated_at == expected.updated_at

    def _assert_source_values(self, actual, expected):
        assert actual.source_id == expected.source_id
        assert actual.name == expected.source
        assert actual.namespace.namespace_id == expected.namespace.namespace_id
        assert actual.owner_email == expected.source_owner_email


class TestRegisterSchema(SchematizerClientTestBase):

    @property
    def yelp_namespace(self):
        return 'yelp_main'

    @property
    def biz_src(self):
        return 'biz'

    @property
    def avro_schema(self):
        return {
            'type': 'record',
            'name': self.biz_src,
            'namespace': self.yelp_namespace,
            'fields': [{'type': 'int', 'name': 'biz_id'}]
        }

    @property
    def avro_schema_str(self):
        return simplejson.dumps(self.avro_schema)

    @property
    def source_owner_email(self):
        return 'dummy@test.com'

    def test_register_schema(self, schematizer):
        actual = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        self._assert_schema_values(actual)

    def test_register_schema_with_base_schema(self, schematizer):
        actual = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=10
        )
        self._assert_schema_values(actual, base_schema_id=10)

    def _assert_schema_values(self, actual, base_schema_id=None):
        assert actual.schema_json == self.avro_schema
        if base_schema_id is None:
            assert actual.base_schema_id is None
        else:
            assert actual.base_schema_id == base_schema_id

        assert not actual.topic.contains_pii

        actual_source = actual.topic.source
        assert actual_source.name == self.biz_src
        assert actual_source.owner_email == self.source_owner_email
        assert actual_source.namespace.name == self.yelp_namespace

    def test_register_same_schema_twice(self, schematizer):
        schema_one = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        schema_two = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        assert schema_one == schema_two

    def test_register_same_schema_with_diff_base_schema(self, schematizer):
        schema_one = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=10
        )
        schema_two = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=20
        )
        self._assert_two_schemas_have_diff_topics(schema_one, schema_two)
        assert schema_one.topic.source == schema_two.topic.source

    def test_register_same_schema_with_diff_pii(self, schematizer):
        schema_one = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        schema_two = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=True
        )
        self._assert_two_schemas_have_diff_topics(schema_one, schema_two)
        assert not schema_one.topic.contains_pii
        assert schema_two.topic.contains_pii
        assert schema_one.topic.source == schema_two.topic.source

    def test_register_same_schema_with_diff_source(self, schematizer):
        another_src = 'biz_user'
        schema_one = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        schema_two = schematizer.register_schema(
            namespace=self.yelp_namespace,
            source=another_src,
            schema_str=self.avro_schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        self._assert_two_schemas_have_diff_topics(schema_one, schema_two)

        src_one = schema_one.topic.source
        src_two = schema_two.topic.source
        assert src_one.name == self.biz_src
        assert src_two.name == another_src
        assert src_one.source_id != src_two.source_id
        assert src_one.namespace.namespace_id == src_two.namespace.namespace_id

    def _assert_two_schemas_have_diff_topics(self, schema_one, schema_two):
        assert schema_one.schema_id != schema_two.schema_id
        assert schema_one.schema_json == schema_two.schema_json

        assert schema_one.topic.topic_id != schema_two.topic.topic_id
        assert schema_one.topic.name != schema_two.topic.name


class TestRegisterSchemaFromMySQL(SchematizerClientTestBase):

    @property
    def yelp_namespace(self):
        return 'yelp_main'

    @property
    def biz_src(self):
        return 'biz'

    @property
    def source_owner_email(self):
        return 'dummy@test.com'

    @property
    def old_create_biz_table_stmt(self):
        return 'create table biz(id int(11) not null);'

    @property
    def alter_biz_table_stmt(self):
        return 'alter table biz;'

    @property
    def new_create_biz_table_stmt(self):
        return 'create table biz(id int(11) not null, name varchar(8));'

    @property
    def avro_schema_of_new_biz_table(self):
        return {
            'type': 'record',
            'name': self.biz_src,
            'namespace': '',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'name',
                 'type': ['null', 'string'], 'maxlen': '8', 'default': None}
            ]
        }

    def test_register_for_new_table(self, schematizer):
        actual = schematizer.register_schema_from_mysql_stmts(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=self.new_create_biz_table_stmt
        )
        self._assert_schema_values(actual)

    def test_register_for_updated_existing_table(self, schematizer):
        actual = schematizer.register_schema_from_mysql_stmts(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=self.new_create_biz_table_stmt,
            old_create_table_stmt=self.old_create_biz_table_stmt,
            alter_table_stmt=self.alter_biz_table_stmt
        )
        self._assert_schema_values(actual)

    def _assert_schema_values(self, actual):
        assert actual.schema_json == self.avro_schema_of_new_biz_table
        assert actual.base_schema_id is None
        assert not actual.topic.contains_pii
        assert actual.topic.source.name == self.biz_src
        assert actual.topic.source.owner_email == self.source_owner_email
        assert actual.topic.source.namespace.name == self.yelp_namespace

    def test_register_same_schema_with_diff_pii(self, schematizer):
        non_pii_schema = schematizer.register_schema_from_mysql_stmts(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=self.new_create_biz_table_stmt
        )

        pii_schema = schematizer.register_schema_from_mysql_stmts(
            namespace=self.yelp_namespace,
            source=self.biz_src,
            source_owner_email=self.source_owner_email,
            contains_pii=True,
            new_create_table_stmt=self.new_create_biz_table_stmt
        )
        assert non_pii_schema.schema_id != pii_schema.schema_id
        assert non_pii_schema.schema_json == pii_schema.schema_json
        assert non_pii_schema.base_schema_id == pii_schema.base_schema_id
        assert non_pii_schema.topic.topic_id != pii_schema.topic.topic_id
        assert non_pii_schema.topic.source == non_pii_schema.topic.source
