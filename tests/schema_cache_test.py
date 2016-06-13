# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import simplejson as json

from data_pipeline.schema_cache import get_schema_cache
from data_pipeline.schema_cache import SchematizerClient


class TestSchemaCache(object):

    @pytest.fixture
    def schema_cache(containers):
        return get_schema_cache()

    @property
    def sample_schema_create_from_mysql_stmts_data(self):
        return {
            'new_create_table_stmt': 'create table foo(id int(11));',
            'namespace': 'test_namespace',
            'source': 'test_source',
            'owner_email': 'test_owner@yelp.com',
            'contains_pii': False
        }

    @pytest.fixture
    def schema_id_and_topic(self, schema_cache):
        return schema_cache.register_schema_from_mysql_stmts(
            **self.sample_schema_create_from_mysql_stmts_data
        )

    def test_schema_cache_deprecated(self):
        with pytest.warns(DeprecationWarning) as deprecation_record:
            SchematizerClient()
        assert (deprecation_record[0].message.args[0] ==
                "data_pipeline.schema_cache.SchematizerClient is deprecated.")

    def test_get_transformed_schema_id(self, schema_cache):
        assert schema_cache.get_transformed_schema_id(0) is None

    def test_get_topic_for_schema_id(self, registered_schema, schema_cache):
        actual_topic_name = schema_cache.get_topic_for_schema_id(
            registered_schema.schema_id
        )
        assert actual_topic_name == registered_schema.topic.name

    def test_get_schema(self, registered_schema, schema_cache):
        actual_schema = schema_cache.get_schema(registered_schema.schema_id)
        assert actual_schema.to_json() == registered_schema.schema_json

    def test_register_transformed_schema(
            self,
            schematizer_client,
            registered_schema,
            schema_cache,
            example_schema
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema=example_schema,
            owner_email='test_owner@yelp.com',
            contains_pii=False
        )
        schema_response = schematizer_client._client.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()
        schema = schema_cache.get_schema(schema_id=schema_id)
        # The loads() calls get around the formatting of json string being
        # different in the response (the objects represented are the same)
        assert json.loads(schema_response.schema) == json.loads(example_schema)
        assert schema_response.schema == schema
        assert schema_response.topic.name == topic
        assert schema_response.topic.source.namespace.name == 'test_namespace'
        assert schema_response.topic.source.name == 'test_source'

    def test_register_transformed_schema_repeated_alternate_source(
            self,
            registered_schema,
            schema_cache,
            example_schema
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema=example_schema,
            owner_email='test_owner@yelp.com',
            contains_pii=False,
        )
        schema_id2, topic2 = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace2',
            source='test_source2',
            schema=example_schema,
            owner_email='test_owner@yelp.com',
            contains_pii=False,
        )
        transformed_id = schema_cache.get_transformed_schema_id(
            schema_id=registered_schema.schema_id
        )
        transformed_topic = schema_cache.get_topic_for_schema_id(
            schema_id=schema_id2
        )
        assert transformed_id == schema_id2
        assert schema_id != schema_id2
        assert topic != topic2
        assert topic2 == transformed_topic

    def test_register_transformed_schema_repeated_same_source(
            self,
            registered_schema,
            schema_cache,
            example_schema
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema=example_schema,
            owner_email='test_owner@yelp.com',
            contains_pii=False,
        )
        schema_id2, topic2 = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema=example_schema,
            owner_email='test_owner@yelp.com',
            contains_pii=False,
        )
        transformed_id = schema_cache.get_transformed_schema_id(
            schema_id=registered_schema.schema_id
        )
        assert transformed_id == schema_id
        assert schema_id == schema_id2
        assert topic == topic2

    def test_register_schema_from_mysql_stmts(
        self,
        schematizer_client,
        schema_cache,
        schema_id_and_topic
    ):
        schema_id, topic = schema_id_and_topic
        schema_response = schematizer_client._client.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()
        schema = schema_cache.get_schema(schema_id=schema_id)
        assert schema_response.schema == schema
        assert schema_response.topic.name == topic
        assert schema_response.topic.contains_pii == \
            self.sample_schema_create_from_mysql_stmts_data['contains_pii']
        assert schema_response.topic.source.namespace.name == \
            self.sample_schema_create_from_mysql_stmts_data['namespace']
        assert schema_response.topic.source.name == \
            self.sample_schema_create_from_mysql_stmts_data['source']
        assert schema_response.topic.source.owner_email == \
            self.sample_schema_create_from_mysql_stmts_data['owner_email']

    def test_register_schema_from_mysql_stmts_alter(
        self,
        schema_cache,
        schema_id_and_topic
    ):
        schema_id_origin, topic_origin = schema_id_and_topic
        schema_id, topic = schema_cache.register_schema_from_mysql_stmts(
            new_create_table_stmt='create table foo(id int(1), val int(1));',
            old_create_table_stmt='create table foo(id int(1));',
            alter_table_stmt='alter table foo add val int(1);',
            namespace='test_namespace',
            source='test_source',
            owner_email='test_owner@yelp.com',
            contains_pii=False
        )
        schema_origin = schema_cache.get_schema(schema_id=schema_id_origin)
        schema_new = schema_cache.get_schema(schema_id=schema_id)
        assert topic_origin == topic
        assert schema_origin != schema_new

    def test_register_schema_from_mysql_stmts_different_pii(
        self,
        schema_cache,
        schema_id_and_topic
    ):
        schema_id_origin, topic_origin = schema_id_and_topic
        schema_id, topic = schema_cache.register_schema_from_mysql_stmts(
            new_create_table_stmt='create table foo(id varchar(1), val int(1));',
            old_create_table_stmt='create table foo(id int(1), val int(1));',
            alter_table_stmt='alter table foo alter column id type varchar(1);',
            namespace='test_namespace',
            source='test_source',
            owner_email='test_owner@yelp.com',
            contains_pii=True
        )
        schema_origin = schema_cache.get_schema(schema_id=schema_id_origin)
        schema_new = schema_cache.get_schema(schema_id=schema_id)
        assert topic_origin != topic
        assert schema_origin != schema_new
