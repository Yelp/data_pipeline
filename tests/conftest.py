# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from uuid import uuid4

import pytest
import simplejson
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import generate_payload_data
from yelp_avro.util import get_avro_schema_object

from data_pipeline.config import configure_from_dict
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


@pytest.fixture(scope="module")
def namespace():
    return 'test_namespace_{}'.format(uuid4())


@pytest.fixture(scope="module")
def source():
    return 'good_source_{}'.format(uuid4())


@pytest.fixture(scope="module")
def example_schema(namespace, source):
    return '''
    {
        "type":"record",
        "namespace": "%s",
        "name": "%s",
        "doc":"test",
        "fields":[
            {"type":"int", "name":"good_field", "doc":"test", "default": 1}
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
def log_source():
    return 'good_log_source_{}'.format(uuid4())


@pytest.fixture(scope="module")
def registered_log_schema(
    schematizer_client,
    example_schema,
    namespace,
    log_source
):
    return schematizer_client.register_schema(
        namespace=namespace,
        source=log_source,
        schema_str=example_schema,
        source_owner_email='test@yelp.com',
        contains_pii=False,
        cluster_type='scribe'
    )


@pytest.fixture(scope='module')
def registered_compatible_schema(
    schematizer_client,
    example_compatible_schema,
    namespace,
    source
):
    return schematizer_client.register_schema(
        namespace=namespace,
        source=source,
        schema_str=example_compatible_schema,
        source_owner_email='test@yelp.com',
        contains_pii=False
    )


@pytest.fixture(scope='module')
def example_compatible_schema(example_schema):
    schema = simplejson.loads(example_schema)
    schema['fields'].append({
        "type": "int",
        "name": "good_compatible_field",
        "doc": "test",
        "default": 1
    })
    return simplejson.dumps(schema)


@pytest.fixture(scope='module')
def example_non_compatible_schema(example_schema):
    schema = simplejson.loads(example_schema)
    schema['fields'].append({
        'doc': 'test',
        'type': 'string',
        'name': 'good_non_compatible_field'
    })
    return simplejson.dumps(schema)


@pytest.fixture(scope="module")
def example_schema_with_pkey(namespace, source):
    return '''
    {
        "type":"record",
        "namespace": "%s",
        "name": "%s",
        "doc":"test",
        "pkey": ["field2", "field1", "field3"],
        "fields":[
            {"type":"int", "name":"field1", "doc":"test", "pkey":2},
            {"type":"string", "name":"field2", "doc":"test", "pkey":1},
            {"type":"int", "name":"field3", "doc":"test", "pkey":3},
            {"type":"int", "name":"field4", "doc":"test"}
        ]
    }
    ''' % (namespace, source)


@pytest.fixture(scope="module")
def registered_schema_with_pkey(
    schematizer_client,
    example_schema_with_pkey,
    namespace,
    source
):
    return schematizer_client.register_schema(
        namespace=namespace,
        source=source,
        schema_str=example_schema_with_pkey,
        source_owner_email='test@yelp.com',
        contains_pii=False
    )


@pytest.fixture
def example_payload_data_with_pkeys(example_schema_with_pkey):
    return generate_payload_data(
        get_avro_schema_object(example_schema_with_pkey)
    )


@pytest.fixture
def example_payload_with_pkeys(
    example_schema_with_pkey,
    example_payload_data_with_pkeys
):
    return AvroStringWriter(
        simplejson.loads(example_schema_with_pkey)
    ).encode(example_payload_data_with_pkeys)


@pytest.fixture
def message_with_pkeys(registered_schema_with_pkey, example_payload_with_pkeys):
    return CreateMessage(
        schema_id=registered_schema_with_pkey.schema_id,
        payload=example_payload_with_pkeys
    )


@pytest.fixture(scope="module")
def pii_schema(schematizer_client, example_schema, namespace):
    return schematizer_client.register_schema(
        namespace=namespace,
        source='pii_source',
        schema_str=example_schema,
        source_owner_email='test@yelp.com',
        contains_pii=True
    )


@pytest.fixture
def registered_meta_attribute(schematizer_client, example_meta_attr_schema, namespace):
    return schematizer_client.register_schema(
        namespace=namespace,
        source='good_meta_attribute',
        schema_str=example_meta_attr_schema,
        source_owner_email='test_meta@yelp.com',
        contains_pii=False
    )


@pytest.fixture
def example_meta_attr_schema(namespace):
    return '''
    {
        "type":"record",
        "namespace":"%s",
        "name":"good_meta_attribute",
        "doc":"test",
        "fields":[
            {"type":"int", "name":"good_payload", "doc":"test"}
        ]
    }
    ''' % (namespace)


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
def compatible_payload_data(example_compatible_schema):
    return {
        "good_field": 1,
        "good_compatible_field": 1
    }


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
def log_message(registered_log_schema, payload):
    return CreateMessage(
        schema_id=registered_log_schema.schema_id,
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


@pytest.yield_fixture(scope='session')
def config_containers_connections():
    configure_from_dict(dict(
        schematizer_host_and_port='schematizer:8888',
        kafka_zookeeper='zk:2181',
        kafka_broker_list=['kafka:9092'],
        should_use_testing_containers=True
    ))
    yield


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
def good_source_ref(good_field_ref, bad_field_ref, namespace, source):
    return {
        "category": "test_category",
        "file_display": "path/to/test.py",
        "fields": [
            good_field_ref,
            bad_field_ref
        ],
        "owner_email": "test@yelp.com",
        "namespace": namespace,
        "file_url": "http://www.test.com/",
        "note": "Notes for good_source",
        "source": source,
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
def schema_ref_defaults(namespace):
    return {
        'doc_owner': 'test_doc_owner@yelp.com',
        'owner_email': 'test_owner@yelp.com',
        'namespace': namespace,
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
