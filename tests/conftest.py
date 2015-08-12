# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from uuid import UUID

import pytest

from data_pipeline._avro_util import AvroStringWriter
from data_pipeline._avro_util import generate_payload_data
from data_pipeline._avro_util import get_avro_schema_object
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.envelope import Envelope
from data_pipeline.message import CreateMessage
from data_pipeline.schema_cache import get_schema_cache
from data_pipeline.schema_ref import SchemaRef
from tests.helpers.containers import Containers
from tests.helpers.kafka_docker import KafkaDocker


logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture
def schema_cache(containers):
    return get_schema_cache()


@pytest.fixture
def schematizer_client(schema_cache):
    return schema_cache.schematizer_client


@pytest.fixture
def registered_schema(schematizer_client, example_schema):
    return schematizer_client.schemas.register_schema(
        body={
            'schema': example_schema,
            'namespace': 'test_namespace',
            'source': 'good_source',
            'source_owner_email': 'test@yelp.com',
            'contains_pii': False
        }
    ).result()


@pytest.fixture
def example_schema():
    return '''
    {
        "type":"record",
        "namespace":"test_namespace",
        "name":"good_source",
        "fields":[
            {"type":"int","name":"good_field"}
        ]
    }
    '''


@pytest.fixture
def example_schema_obj(example_schema):
    return get_avro_schema_object(example_schema)


@pytest.fixture
def example_payload_data(example_schema_obj):
    return generate_payload_data(example_schema_obj)


@pytest.fixture
def payload(example_schema_obj, example_payload_data):
    return AvroStringWriter(example_schema_obj).encode(example_payload_data)


@pytest.fixture
def example_previous_payload_data(example_schema_obj):
    return generate_payload_data(example_schema_obj)


@pytest.fixture(scope='module')
def topic_name():
    return str(UUID(bytes=FastUUID().uuid4()).hex)


@pytest.fixture
def message(topic_name, payload, registered_schema, example_payload_data):
    msg = CreateMessage(
        topic=topic_name,
        schema_id=registered_schema.schema_id,
        payload=payload,
        timestamp=1500
    )
    # TODO [DATAPIPE-249|clin] as part of refactoring and cleanup consumer
    # tests, let's re-visit and see if these assertions are needed.
    assert msg.topic == topic_name
    assert msg.schema_id == registered_schema.schema_id
    assert msg.payload == payload
    assert msg.payload_data == example_payload_data
    return msg


@pytest.fixture
def message_with_payload_data(topic_name, registered_schema):
    return CreateMessage(
        topic=topic_name,
        schema_id=registered_schema.schema_id,
        payload_data={'test': 100},
        timestamp=1500
    )


@pytest.fixture
def envelope():
    return Envelope()


@pytest.yield_fixture(scope='session')
def containers():
    with Containers():
        yield


@pytest.fixture(scope='session')
def kafka_docker(containers):
    return KafkaDocker.get_connection()


@pytest.fixture
def good_source_ref():
    return {
        "category": "test_category",
        "file_display": "path/to/test.py",
        "fields": [
            {
                "note": "Notes for good_field",
                "doc": "Docs for good_field",
                "name": "good_field"
            },
            {
                "name": "bad_field"
            },
        ],
        "owner_email": "test@yelp.com",
        "namespace": "test_namespace",
        "file_url": "http://www.test.com/",
        "note": "Notes for good_source",
        "source": "good_source",
        "doc": "Docs for good_source",
        "contains_pii": False
    }


@pytest.fixture
def bad_source_ref():
    return {"fields": [], "source": "bad_source"}


@pytest.fixture
def schema_ref_dict(good_source_ref, bad_source_ref):
    return {
        "doc_source": "http://www.docs-r-us.com/good",
        "docs": [
            good_source_ref,
            bad_source_ref
        ],
        "doc_owner": "test@yelp.com"
    }


@pytest.fixture
def schema_ref_defaults():
    return {
        'doc_owner': 'test_doc_owner@yelp.com',
        'owner_email': 'test_owner@yelp.com',
        'namespace': 'test_namespace',
        'doc': 'test_doc',
        'contains_pii': False,
        'category': 'test_category'
    }


@pytest.fixture
def schema_ref(schema_ref_dict, schema_ref_defaults):
    return SchemaRef(
        schema_ref=schema_ref_dict,
        defaults=schema_ref_defaults
    )
