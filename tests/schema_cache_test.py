# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import simplejson as json

from data_pipeline.schema_cache import get_schema_cache


class TestSchemaCache(object):

    @pytest.fixture
    def example_schema(self):
        return b'''
        {"type":"record","namespace":"test","name":"test","fields":[
            {"type":"int","name":"test"}
        ]}
        '''

    @pytest.fixture
    def api(self, schema_cache):
        return schema_cache.schematizer_client

    @pytest.fixture
    def registered_schema(self, api, example_schema):
        return api.schemas.register_schema(
            body={
                'schema': example_schema,
                'namespace': 'test',
                'source': 'test',
                'source_owner_email': 'test@yelp.com'
            }
        ).result()

    @pytest.fixture
    def schema_cache(self):
        return get_schema_cache()

    def test_get_transformed_schema_id(self, schema_cache):
        assert schema_cache.get_transformed_schema_id(0) is None

    def test_get_topic_for_schema_id(self, registered_schema, schema_cache):
        actual_topic_name = schema_cache.get_topic_for_schema_id(
            registered_schema.schema_id
        )
        assert actual_topic_name == registered_schema.topic.name

    def test_get_schema(self, registered_schema, schema_cache):
        actual_schema = schema_cache.get_schema(registered_schema.schema_id)
        assert actual_schema == registered_schema.schema

    def test_register_transformed_schema(
            self,
            api,
            registered_schema,
            schema_cache,
            example_schema
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema=example_schema,
            owner_email='test_owner@yelp.com'
        )
        schema_response = api.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()
        schema = schema_cache.get_schema(schema_id=schema_id)
        # The loads() calls get around the formatting of json string being
        # different in the response (the objects represented are the same)
        assert json.loads(schema_response.schema) == json.loads(example_schema)
        assert schema_response.schema == schema
        assert schema_response.topic.name == topic
        assert schema_response.topic.source.namespace == 'test_namespace'
        assert schema_response.topic.source.source == 'test_source'

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
            owner_email='test_owner@yelp.com'
        )
        schema_id2, topic2 = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace2',
            source='test_source2',
            schema=example_schema,
            owner_email='test_owner@yelp.com'
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
            owner_email='test_owner@yelp.com'
        )
        schema_id2, topic2 = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema=example_schema,
            owner_email='test_owner@yelp.com'
        )
        transformed_id = schema_cache.get_transformed_schema_id(
            schema_id=registered_schema.schema_id
        )
        assert transformed_id == schema_id
        assert schema_id == schema_id2
        assert topic == topic2
