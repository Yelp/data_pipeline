# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from uuid import uuid4

import mock
import pytest
import simplejson
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import generate_payload_data
from yelp_avro.util import get_avro_schema_object

import data_pipeline._fast_uuid
from data_pipeline._fast_uuid import FastUUID
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
        "fields":[
            {"type":"int", "name":"good_payload"}
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


@pytest.fixture(params=[True, False])
def libuuid_available(request):
    return request.param


@pytest.yield_fixture
def fast_uuid(libuuid_available):
    if libuuid_available:
        yield FastUUID()
    else:
        with mock.patch.object(
            data_pipeline._fast_uuid,
            'FFI',
            side_effect=Exception
        ):
            # Save and restore the existing state; this will allow already
            # instantiated FastUUID instances to keep working.
            original_ffi = data_pipeline._fast_uuid._LibUUID._ffi
            data_pipeline._fast_uuid._LibUUID._ffi = None
            try:
                yield FastUUID()
            finally:
                data_pipeline._fast_uuid._LibUUID._ffi = original_ffi
