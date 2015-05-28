# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.config import get_schematizer_client
from data_pipeline.sample_data_loader import SampleDataLoader
from data_pipeline.schema_cache import SchemaCache


class TestSchemaCache(object):

    @pytest.fixture
    def api(self):
        return get_schematizer_client()

    @pytest.fixture
    def registered_schema(self, api):
        return api.schemas.register_avro_schema(
            body={
                'schema': SampleDataLoader().get_data('raw_business.avsc'),
                'namespace': 'yelp_db',
                'source': 'business',
                'source_owner_email': 'test@yelp.com'
            }
        ).result()

    @pytest.fixture
    def schema_cache(self, api):
        return SchemaCache(schematizer_client=api)

    def test_get_transformed_schema_id(self, schema_cache):
        assert schema_cache.get_transformed_schema_id(0) is None

    def test_get_topic_for_schema_id(self, registered_schema, schema_cache):
        topic_resp = schema_cache.get_topic_for_schema_id(
            registered_schema.schema_id)
        assert topic_resp == registered_schema.topic

    def test_get_schema(self, registered_schema, schema_cache):
        schema = schema_cache.get_schema(registered_schema.schema_id)
        assert registered_schema.schema == schema

    def test_register_transformed_schema(
            self, api, registered_schema, schema_cache
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema='test schema',
            owner_email='test_owner@yelp.com'
        )
        schema_response = api.get_schema_by_id(schema_id=schema_id)
        schema = schema_cache.get_schema(schema_id=schema_id)
        assert schema_response.schema == 'test schema'
        assert schema_response.schema == schema
        assert schema_response.topic == topic
        assert schema_response.namespace == 'test_namespace'
        assert schema_response.source == 'test_source'

    def test_register_transformed_schema_repeated_alternate_source(
            self, registered_schema, schema_cache
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema='test schema',
            owner_email='test_owner@yelp.com'
        )
        schema_id2, topic2 = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace2',
            source='test_source2',
            schema='test schema',
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
            self, registered_schema, schema_cache
    ):
        schema_id, topic = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema='test schema',
            owner_email='test_owner@yelp.com'
        )
        schema_id2, topic2 = schema_cache.register_transformed_schema(
            base_schema_id=registered_schema.schema_id,
            namespace='test_namespace',
            source='test_source',
            schema='test schema',
            owner_email='test_owner@yelp.com'
        )
        transformed_id = schema_cache.get_transformed_schema_id(
            schema_id=registered_schema.schema_id
        )
        assert transformed_id == schema_id
        assert schema_id == schema_id2
        assert topic == topic2
