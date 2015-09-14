# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from uuid import UUID

import pytest

from data_pipeline._avro_util import AvroStringWriter
from data_pipeline._avro_util import generate_payload_data
from data_pipeline._avro_util import get_avro_schema_object
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.envelope import Envelope
from data_pipeline.message import CreateMessage
from data_pipeline.schema_cache import get_schema_cache
from data_pipeline.testing_helpers.kafka_docker import KafkaDocker
from tests.helpers.config import reconfigure
from tests.helpers.containers import Containers


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


@pytest.fixture()
def team_name():
    return 'bam'


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
    with Containers() as containers:
        with containers.use_testing_containers():
            yield


@pytest.fixture(scope='session')
def kafka_docker(containers):
    return KafkaDocker.get_connection()


@pytest.yield_fixture(scope='session')
def configure_teams():
    config_path = os.path.join(
        os.path.dirname(__file__),
        'config/teams.yaml'
    )
    with reconfigure(data_pipeline_teams_config_file_path=config_path):
        yield
