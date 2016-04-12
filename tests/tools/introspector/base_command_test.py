# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

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
from data_pipeline.tools.introspector.base import IntrospectorBatch


@pytest.mark.usefixtures('containers', "configure_teams")
class TestBaseCommand(object):

    def _get_client(self):
        return get_config().schematizer_client

    def _register_avro_schema(self, namespace, source, two_fields, **overrides):
        fields = [{'type': 'int', 'name': 'foo'}]
        if two_fields:
            fields.append({'type': 'int', 'name': 'bar'})
        schema_json = {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'fields': fields
        }
        params = {
            'namespace': namespace,
            'source': source,
            'source_owner_email': self.source_owner_email,
            'schema': simplejson.dumps(schema_json),
            'contains_pii': False
        }
        if overrides:
            params.update(**overrides)
        return self._get_client().schemas.register_schema(body=params).result()

    def _create_payload(self, schema):
        avro_schema_obj = get_avro_schema_object(
            schema.schema
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

    @pytest.fixture
    def batch(self, containers):
        batch = IntrospectorBatch("data_pipeline_introspector_base")
        batch.log.debug = mock.Mock()
        batch.log.info = mock.Mock()
        batch.log.warning = mock.Mock()
        return batch

    @pytest.yield_fixture
    def producer(self, containers, team_name):
        instance = Producer(
            producer_name="introspector_producer",
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False,
            monitoring_enabled=False
        )
        with instance as producer:
            yield producer

    @property
    def source_owner_email(self):
        return "bam+test+introspection@yelp.com"

    @pytest.fixture
    def namespace_one(self):
        return "namespace_one"

    @pytest.fixture
    def namespace_two(self):
        return "namespace_two"

    @pytest.fixture
    def source_one_active(self):
        return "source_one_active"

    @pytest.fixture
    def source_one_inactive(self):
        return "source_one_inactive"

    @pytest.fixture
    def source_two_active(self):
        return "source_two_active"

    @pytest.fixture
    def source_two_inactive(self):
        return "source_two_inactive"

    @pytest.fixture
    def schema_one_active(self, containers, namespace_one, source_one_active):
        return self._register_avro_schema(namespace_one, source_one_active, two_fields=False)

    @pytest.fixture
    def schema_one_inactive(self, containers, namespace_one, source_one_inactive):
        return self._register_avro_schema(namespace_one, source_one_inactive, two_fields=True)

    @pytest.fixture
    def schema_two_active(self, containers, namespace_two, source_two_active):
        return self._register_avro_schema(namespace_two, source_two_active, two_fields=False)

    @pytest.fixture
    def schema_two_inactive(self, containers, namespace_two, source_two_inactive):
        return self._register_avro_schema(namespace_two, source_two_inactive, two_fields=True)

    @pytest.fixture(autouse=True)
    def topic_one_inactive(self, containers, schema_one_inactive):
        topic = schema_one_inactive.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True)
    def topic_one_active(self, containers, schema_one_active):
        topic = schema_one_active.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True)
    def topic_two_inactive(self, containers, schema_two_inactive):
        topic = schema_two_inactive.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True)
    def topic_two_active(self, containers, schema_two_active):
        topic = schema_two_active.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture
    def payload_one(self, schema_one_active):
        return self._create_payload(schema_one_active)

    @pytest.fixture
    def payload_two(self, schema_two_active):
        return self._create_payload(schema_two_active)

    @pytest.fixture(autouse=True)
    def message_one(
        self,
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

    @pytest.fixture(autouse=True)
    def message_two(
        self,
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
    def inactive_topics(self, topic_one_inactive, topic_two_inactive):
        return [topic_one_inactive, topic_two_inactive]

    @pytest.fixture
    def active_sources(self, source_one_active, source_two_active):
        return [source_one_active, source_two_active]

    @pytest.fixture
    def inactive_sources(self, source_one_inactive, source_two_inactive):
        return [source_one_inactive, source_two_inactive]

    @pytest.fixture
    def namespaces(self, namespace_one, namespace_two):
        return [namespace_one, namespace_two]

    def test_active_topics(
        self,
        batch,
        active_topics,
        inactive_topics
    ):
        found_actives = 0
        found_inactives = 0
        active_topic_names = [topic.name for topic in active_topics]
        inactive_topic_names = [topic.name for topic in inactive_topics]
        for topic in batch.active_topics:
            topic_name = topic['name']
            if topic_name in active_topic_names:
                found_actives += 1
            if topic_name in inactive_topic_names:
                found_inactives += 1
        assert found_actives == len(active_topics)
        assert not found_inactives

    def test_active_sources(
        self,
        batch,
        active_topics,
        inactive_topics
    ):
        actual_active_sources = batch.active_sources
        active_sources = [topic.source for topic in active_topics]
        inactive_sources = [topic.source for topic in inactive_topics]
        for source in active_sources:
            assert source.source_id in actual_active_sources.keys()
            assert actual_active_sources[
                source.source_id
            ]['active_topic_count'] == 1
        for source in inactive_sources:
            assert source.source_id not in actual_active_sources.keys()

    def test_active_namespaces(
        self,
        batch,
        namespaces
    ):
        actual_active_namespaces = batch.active_namespaces
        for namespace in namespaces:
            assert namespace in actual_active_namespaces.keys()
            assert actual_active_namespaces[
                namespace
            ]['active_topic_count'] == 1
            assert actual_active_namespaces[
                namespace
            ]['active_source_count'] == 1

    def test_topic_to_dict(
        self,
        batch,
        topic_one_active,
        topic_two_inactive,
        namespace_one,
        namespace_two,
        source_one_active,
        source_two_inactive
    ):
        dict_one = batch.topic_to_dict(topic_one_active)
        dict_two = batch.topic_to_dict(topic_two_inactive)
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=dict_one,
            namespace_name=namespace_one,
            source_name = source_one_active,
            is_active=True
        )
        self._assert_topic_equals_topic_dict(
            topic=topic_two_inactive,
            topic_dict=dict_two,
            namespace_name=namespace_two,
            source_name = source_two_inactive,
            is_active=False
        )

    def _assert_topic_equals_topic_dict(
        self,
        topic,
        topic_dict,
        namespace_name,
        source_name,
        is_active
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
        assert topic_dict['in_kafka']
        assert (
            (is_active and topic_dict['message_count']) or
            (not is_active and not topic_dict['message_count'])
        )
