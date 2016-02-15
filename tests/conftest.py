# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import pytest
import simplejson
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import generate_payload_data
from yelp_avro.util import get_avro_schema_object

from data_pipeline.message import CreateMessage
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.testing_helpers.containers import Containers
from tests.helpers.config import reconfigure


logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture(scope="module")
def schematizer_client(containers):
    return get_schematizer()


@pytest.fixture(scope="module")
def namespace():
    return 'test_namespace'


@pytest.fixture(scope="module")
def source():
    return 'good_source'


@pytest.fixture(scope="module")
def example_schema(namespace, source):
    return '''
    {
        "type":"record",
        "namespace": "%s",
        "name": "%s",
        "fields":[
            {"type":"int", "name":"good_field"}
        ]
    }
    ''' % (namespace, source)


@pytest.fixture(scope="module")
def registered_schema(schematizer_client, example_schema, namespace, source):
    return schematizer_client.register_schema(
        namespace=namespace,
        source=source,
        schema_str=example_schema,
        source_owner_email='test@yelp.com',
        contains_pii=False
    )


@pytest.fixture(scope="module")
def pii_schema(schematizer_client, example_schema):
    return schematizer_client.register_schema(
        namespace='test_namespace',
        source='pii_source',
        schema_str=example_schema,
        source_owner_email='test@yelp.com',
        contains_pii=True
    )


@pytest.fixture
def registered_meta_attribute(schematizer_client, example_meta_attr_schema):
    return schematizer_client.register_schema(
        namespace='test_namespace',
        source='good_meta_attribute',
        schema_str=example_meta_attr_schema,
        source_owner_email='test_meta@yelp.com',
        contains_pii=False
    )


@pytest.fixture
def example_meta_attr_schema():
    return '''
    {
        "type":"record",
        "namespace":"test_namespace",
        "name":"good_meta_attribute",
        "fields":[
            {"type":"int", "name":"good_payload"}
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
def payload(example_schema, example_payload_data):
    return AvroStringWriter(
        simplejson.loads(example_schema)
    ).encode(example_payload_data)


@pytest.fixture
def example_previous_payload_data(example_schema_obj):
    return generate_payload_data(example_schema_obj)


@pytest.fixture
def previous_payload(example_schema, example_previous_payload_data):
    return AvroStringWriter(
        simplejson.loads(example_schema)
    ).encode(example_previous_payload_data)


@pytest.fixture()
def team_name():
    return 'bam'


@pytest.fixture
def message(registered_schema, payload):
    return CreateMessage(
        schema_id=registered_schema.schema_id,
        payload=payload
    )


@pytest.fixture
def payload_data_message(registered_schema, example_payload_data):
    return CreateMessage(
        schema_id=registered_schema.schema_id,
        payload_data=example_payload_data
    )


@pytest.yield_fixture(scope='session')
def containers():
    with Containers() as containers:
        yield containers


@pytest.fixture(scope='session')
def kafka_docker(containers):
    return containers.get_kafka_connection()


@pytest.yield_fixture
def configure_teams():
    config_path = os.path.join(
        os.path.dirname(__file__),
        'config/teams.yaml'
    )
    with reconfigure(data_pipeline_teams_config_file_path=config_path):
        yield
