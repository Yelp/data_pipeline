# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json

import pytest
from frozendict import frozendict

from data_pipeline.config import get_config
from data_pipeline.helpers.frozendict_json_encoder import FrozenDictEncoder
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore


class TestAvroStringStore(object):

    @pytest.fixture
    def schematizer(self, containers):
        return get_config().schematizer_client

    @pytest.fixture(params=[
        {
            'type': 'bytes',
            'logicalType': 'decimal',
            'precision': 4,
            'scale': 2
        },
        frozendict(
            {
                'type': 'bytes',
                'logicalType': 'decimal',
                'precision': 4,
                'scale': 2
            }
        )
    ])
    def parameters(self, request):
        return {
            'namespace': 'foo_ns',
            'source': 'foo_source',
            'source_owner_email': 'footastic@yelp.com',
            'contains_pii': False,
            'schema': json.dumps(request.param, cls=FrozenDictEncoder)
        }

    @pytest.fixture(params=[
        {
            'type': 'record',
            'name': 'estimated_pricing',
            'namespace': 'estimated_auction_pricing_worker',
            'fields': [
                {
                    'type': 'string',
                    'name': 'business_encid'
                },
                {
                    'type': ['null', 'string'],
                    'name': 'estimation_method'
                }
            ]
        },
        frozendict(
            {
                'type': 'record',
                'name': 'estimated_pricing',
                'namespace': 'estimated_auction_pricing_worker',
                'fields': [
                    {
                        'type': 'string',
                        'name': 'business_encid'
                    },
                    {
                        'type': ['null', 'string'],
                        'name': 'estimation_method'
                    }
                ]
            }
        )
    ])
    def schema_types(self, request):
        return request.param

    def test_get_writer_without_schema(self, schematizer, parameters):
        registered_schema = schematizer.schemas.register_schema(
            body=parameters
        ).result()
        schema_id = registered_schema.schema_id
        store = _AvroStringStore()
        store.get_writer(schema_id)
        assert schema_id in store._writer_cache

    def test_get_writer_with_schema(self, schema_types):
        schema_id = 5
        store = _AvroStringStore()
        store.get_writer(schema_id, schema_types)
        assert schema_id in store._writer_cache

    def test_get_reader_without_schema(self, schematizer, parameters):
        registered_schema = schematizer.schemas.register_schema(
            body=parameters
        ).result()
        schema_id = registered_schema.schema_id
        store = _AvroStringStore()
        store.get_reader(schema_id, schema_id)
        assert (schema_id, schema_id) in store._reader_cache

    def test_get_reader_with_schema(self, schema_types):
        schema_id = 5
        store = _AvroStringStore()
        store.get_reader(schema_id, schema_id, schema_types, schema_types)
        assert (schema_id, schema_id) in store._reader_cache
