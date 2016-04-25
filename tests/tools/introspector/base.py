# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple
from uuid import uuid4

import mock
import pytest
import simplejson
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import \
    generate_payload_data
from yelp_avro.util import get_avro_schema_object

from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.introspector.base import IntrospectorCommand


class FakeParserError(Exception):
    pass

@pytest.mark.usefixtures('containers')
class TestIntrospectorBase(object):
    def _register_avro_schema(self, namespace, source, two_fields):
        fields = [{'type': 'int', 'name': 'foo'}]
        if two_fields:
            fields.append({'type': 'int', 'name': 'bar'})
        schema_json = {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'fields': fields
        }
        return get_schematizer().register_schema(
            namespace=namespace,
            source=source,
            schema_str=simplejson.dumps(schema_json),
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=None
        )

    def _create_payload(self, schema):
        avro_schema_obj = get_avro_schema_object(
            simplejson.dumps(schema.schema_json)
        )
        payload_data = generate_payload_data(avro_schema_obj)
        return AvroStringWriter(avro_schema_obj).encode(payload_data)

    def _create_message(
        self,
        topic,
        schema,
        payload,
        **kwargs
    ):
        return CreateMessage(
            topic=str(topic.name),
            schema_id=schema.schema_id,
            payload=payload,
            timestamp=1500,
            **kwargs
        )

    @pytest.yield_fixture(scope='class')
    def producer(self, containers):
        instance = Producer(
            producer_name="introspector_producer",
            team_name="bam",
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False,
            monitoring_enabled=False
        )
        with instance as producer:
            yield producer

    @pytest.fixture(scope='class')
    def schematizer(self, containers):
        return get_schematizer()

    @pytest.fixture
    def parser(self):
        parser = mock.Mock()
        parser.error = FakeParserError
        return parser

    @property
    def source_owner_email(self):
        return "bam+test+introspection@yelp.com"

    @pytest.fixture(scope='class')
    def namespace_one(self):
        return "introspector_namespace_one_{}".format(uuid4())

    @pytest.fixture(scope='class')
    def namespace_two(self):
        return "introspector_namespace_two_{}".format(uuid4())

    @pytest.fixture(scope='class')
    def source_one_active(self):
        return "introspector_source_one_active_{}".format(uuid4())

    @pytest.fixture(scope='class')
    def source_one_inactive(self):
        return "introspector_source_one_inactive_{}".format(uuid4())

    @pytest.fixture(scope='class')
    def source_two_active(self):
        return "introspector_source_two_active_{}".format(uuid4())

    @pytest.fixture(scope='class')
    def source_two_inactive(self):
        return "introspector_source_two_inactive_{}".format(uuid4())

    @pytest.fixture(scope='class')
    def schema_one_active(self, containers, namespace_one, source_one_active):
        return self._register_avro_schema(namespace_one, source_one_active, two_fields=False)

    @pytest.fixture(scope='class')
    def schema_one_inactive(self, containers, namespace_one, source_one_inactive):
        return self._register_avro_schema(namespace_one, source_one_inactive, two_fields=True)

    @pytest.fixture(scope='class')
    def schema_one_inactive_b(self, containers, namespace_one, source_one_inactive):
        return self._register_avro_schema(namespace_one, source_one_inactive, two_fields=False)

    @pytest.fixture(scope='class')
    def schema_two_active(self, containers, namespace_two, source_two_active):
        return self._register_avro_schema(namespace_two, source_two_active, two_fields=False)

    @pytest.fixture(scope='class')
    def schema_two_inactive(self, containers, namespace_two, source_two_inactive):
        return self._register_avro_schema(namespace_two, source_two_inactive, two_fields=True)

    @pytest.fixture(autouse=True, scope='class')
    def topic_one_inactive(self, containers, schema_one_inactive):
        topic = schema_one_inactive.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True, scope='class')
    def topic_one_inactive_b(self, containers, schema_one_inactive_b):
        topic = schema_one_inactive_b.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True, scope='class')
    def topic_one_active(self, containers, schema_one_active):
        topic = schema_one_active.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True, scope='class')
    def topic_two_inactive(self, containers, schema_two_inactive):
        topic = schema_two_inactive.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True, scope='class')
    def topic_two_active(self, containers, schema_two_active):
        topic = schema_two_active.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(scope='class')
    def payload_one(self, schema_one_active):
        return self._create_payload(schema_one_active)

    @pytest.fixture(scope='class')
    def payload_two(self, schema_two_active):
        return self._create_payload(schema_two_active)

    @pytest.fixture(autouse=True, scope='class')
    def message_one(
        self,
        containers,
        producer,
        topic_one_active,
        schema_one_active,
        payload_one
    ):
        message = self._create_message(
            topic=topic_one_active,
            schema=schema_one_active,
            payload=payload_one
        )
        producer.publish(message)
        producer.flush()
        return message

    @pytest.fixture(autouse=True, scope='class')
    def message_two(
        self,
        containers,
        producer,
        topic_two_active,
        schema_two_active,
        payload_two
    ):
        message = self._create_message(
            topic=topic_two_active,
            schema=schema_two_active,
            payload=payload_two
        )
        producer.publish(message)
        producer.flush()
        return message

    @pytest.fixture
    def active_topics(self, topic_one_active, topic_two_active):
        return [topic_one_active, topic_two_active]

    @pytest.fixture
    def inactive_topics(self, topic_one_inactive, topic_one_inactive_b, topic_two_inactive):
        return [topic_one_inactive, topic_one_inactive_b, topic_two_inactive]

    @pytest.fixture
    def active_sources(self, source_one_active, source_two_active):
        return [source_one_active, source_two_active]

    @pytest.fixture
    def inactive_sources(self, source_one_inactive, source_two_inactive):
        return [source_one_inactive, source_two_inactive]

    @pytest.fixture
    def sources(self, active_sources, inactive_sources):
        return active_sources + inactive_sources

    @pytest.fixture
    def namespaces(self, namespace_one, namespace_two):
        return [namespace_one, namespace_two]

    def _assert_topic_equals_topic_dict(
        self,
        topic,
        topic_dict,
        namespace_name,
        source_name,
        is_active,
        include_kafka_info=True
    ):
        fields = [
            'name',
            'topic_id',
            'primary_keys',
            'contains_pii',
        ]
        for field in fields:
            assert topic_dict[field] == getattr(topic, field)
        assert topic_dict['source_name'] == source_name
        assert topic_dict['source_id'] == topic.source.source_id
        assert topic_dict['namespace'] == namespace_name
        if include_kafka_info:
            assert topic_dict['in_kafka']
            assert (
                (is_active and topic_dict['message_count']) or
                (not is_active and not topic_dict['message_count'])
            )
        else:
            with pytest.raises(KeyError):
                topic_dict['in_kafka']
            with pytest.raises(KeyError):
                topic_dict['message_count']

    def _assert_schema_equals_schema_dict(
        self,
        topic_schema,
        schema_obj,
        schema_dict,
        topic_to_check=None
    ):
        assert topic_schema.schema_id == schema_dict['schema_id']
        assert topic_schema.base_schema_id == schema_dict['base_schema_id']
        assert topic_schema.primary_keys == schema_dict['primary_keys']
        assert topic_schema.note == schema_dict['note']
        assert schema_obj.schema_json == schema_dict['schema_json']
        assert schema_obj.status == schema_dict['status']
        if topic_to_check:
            self._assert_topic_equals_topic_dict(
                topic=topic_to_check,
                topic_dict=schema_dict['topic'],
                namespace_name=topic_to_check.source.namespace.name,
                source_name=topic_to_check.source.name,
                is_active=False,
                include_kafka_info=False
            )
        else:
            with pytest.raises(KeyError):
                schema_dict['topic']

    def _assert_source_equals_source_dict(
        self,
        source,
        source_dict,
        namespace_name,
        source_name,
        active_topic_count
    ):
        fields = [
            'name',
            'source_id',
            'owner_email'
        ]
        for field in fields:
            assert source_dict[field] == getattr(source, field)
        assert source_dict['active_topic_count'] == active_topic_count
        assert source_dict['namespace'] == namespace_name
        assert source_dict['name'] == source_name

    def _assert_namespace_equals_namespace_dict(
        self,
        namespace,
        namespace_dict,
        namespace_name
    ):
        assert namespace_dict['namespace_id'] == namespace.namespace_id
        assert namespace_dict['name'] == namespace_name
        assert namespace_dict['active_topic_count'] == 1
        assert namespace_dict['active_source_count'] == 1
