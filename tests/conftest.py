# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import pytest

from data_pipeline._avro_util import AvroStringWriter
from data_pipeline._avro_util import generate_payload_data
from data_pipeline._avro_util import get_avro_schema_object
from data_pipeline.envelope import Envelope
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from data_pipeline.schema_cache import get_schema_cache
from tests.helpers.kafka_docker import KafkaDocker

logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture
def schema_cache():
    return get_schema_cache()


@pytest.fixture
def schematizer_client(schema_cache):
    return schema_cache.schematizer_client


@pytest.fixture
def registered_schema(schematizer_client, example_schema):
    return schematizer_client.schemas.register_schema(
        body={
            'schema': example_schema,
            'namespace': 'test',
            'source': 'test',
            'source_owner_email': 'test@yelp.com'
        }
    ).result()


@pytest.fixture
def example_schema():
    return '''
    {"type":"record","namespace":"test","name":"test","fields":[
        {"type":"int","name":"test"}
    ]}
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


@pytest.fixture(scope='module')
def topic_name():
    return str('my-topic')


@pytest.fixture
def message(topic_name, payload, registered_schema):
    return Message(
        topic=topic_name,
        schema_id=registered_schema.schema_id,
        payload=payload,
        message_type=MessageType.create
    )


@pytest.fixture
def envelope():
    return Envelope()


@pytest.yield_fixture(scope='session')
def kafka_docker():
    with KafkaDocker() as get_connection:
        yield get_connection()
