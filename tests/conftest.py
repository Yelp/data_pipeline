# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from uuid import UUID

import pytest
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import \
    generate_payload_data
from yelp_avro.util import get_avro_schema_object

from data_pipeline._fast_uuid import FastUUID
from data_pipeline.envelope import Envelope
from data_pipeline.message import CreateMessage
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.testing_helpers.containers import Containers
from data_pipeline.tools.schema_ref import SchemaRef
from tests.helpers.config import reconfigure


logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture(scope='session')
def schematizer_client(containers):
    return get_schematizer()


@pytest.fixture
def namespace():
    return 'test_namespace'


@pytest.fixture
def source():
    return 'good_source'


@pytest.fixture
def registered_schema(schematizer_client, example_schema, namespace, source):
    return schematizer_client.register_schema(
        namespace=namespace,
        source=source,
        schema_str=example_schema,
        source_owner_email='test@yelp.com',
        contains_pii=False
    )


@pytest.fixture
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
def payload(example_schema_obj, example_payload_data):
    return AvroStringWriter(example_schema_obj).encode(example_payload_data)


@pytest.fixture
def example_previous_payload_data(example_schema_obj):
    return generate_payload_data(example_schema_obj)


@pytest.fixture(scope='module')
def topic_name():
    return str(UUID(bytes=FastUUID().uuid4()).hex)


@pytest.fixture(scope='module')
def topic_two_name():
    return str(UUID(bytes=FastUUID().uuid4()).hex)


@pytest.fixture(scope='module')
def topic(containers, topic_name):
    containers.create_kafka_topic(topic_name)
    return topic_name


@pytest.fixture(scope='module')
def topic_two(containers, topic_two_name):
    containers.create_kafka_topic(topic_two_name)
    return topic_two_name


@pytest.fixture()
def team_name():
    return 'bam'


@pytest.fixture
def message(topic_name, payload, registered_schema, example_payload_data):
    msg = CreateMessage(
        topic=topic_name,
        schema_id=registered_schema.schema_id,
        payload=payload,
        timestamp=1500,
        contains_pii=False
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
        payload_data={'good_field': 100},
        timestamp=1500,
        contains_pii=False
    )


@pytest.fixture
def envelope():
    return Envelope()


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


@pytest.fixture
def bad_field_ref():
    return {
        "name": "bad_field"
    }


@pytest.fixture
def good_field_ref():
    return {
        "note": "Notes for good_field",
        "doc": "Docs for good_field",
        "name": "good_field"
    }


@pytest.fixture
def good_source_ref(good_field_ref, bad_field_ref):
    return {
        "category": "test_category",
        "file_display": "path/to/test.py",
        "fields": [
            good_field_ref,
            bad_field_ref
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
