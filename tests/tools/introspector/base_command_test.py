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
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.introspector.base import IntrospectorBatch


@pytest.mark.usefixtures('containers')
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

    @property
    def source_owner_email(self):
        return "bam+test+introspection@yelp.com"

    @pytest.fixture(scope='class')
    def namespace_one(self):
        return "introspector_namespace_one"

    @pytest.fixture(scope='class')
    def namespace_two(self):
        return "introspector_namespace_two"

    @pytest.fixture(scope='class')
    def source_one_active(self):
        return "introspector_source_one_active"

    @pytest.fixture(scope='class')
    def source_one_inactive(self):
        return "introspector_source_one_inactive"

    @pytest.fixture(scope='class')
    def source_two_active(self):
        return "introspector_source_two_active"

    @pytest.fixture(scope='class')
    def source_two_inactive(self):
        return "introspector_source_two_inactive"

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

    def test_schema_to_dict(
        self,
        batch,
        schematizer,
        schema_one_active,
        topic_one_active
    ):
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        actual_schema_dict = batch.schema_to_dict(topic_schema)
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schema_dict
        )

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
            source_name=source_one_active,
            is_active=True
        )
        self._assert_topic_equals_topic_dict(
            topic=topic_two_inactive,
            topic_dict=dict_two,
            namespace_name=namespace_two,
            source_name=source_two_inactive,
            is_active=False
        )

    def test_source_to_dict(
        self,
        batch,
        source_one_active,
        source_two_inactive,
        topic_one_active,
        topic_two_inactive,
        namespace_one,
        namespace_two
    ):
        source_one_obj = topic_one_active.source
        source_two_obj = topic_two_inactive.source
        source_one_dict = batch.source_to_dict(source_one_obj)
        source_two_dict = batch.source_to_dict(source_two_obj)
        self._assert_source_equals_source_dict(
            source=source_one_obj,
            source_dict=source_one_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            active_topic_count=1
        )
        self._assert_source_equals_source_dict(
            source=source_two_obj,
            source_dict=source_two_dict,
            namespace_name=namespace_two,
            source_name=source_two_inactive,
            active_topic_count=0
        )

    def test_namespace_to_dict(
        self,
        batch,
        topic_one_active,
        topic_two_active,
        namespace_one,
        namespace_two
    ):
        namespace_one_obj = topic_one_active.source.namespace
        namespace_two_obj = topic_two_active.source.namespace
        namespace_one_dict = batch.namespace_to_dict(namespace_one_obj)
        namespace_two_dict = batch.namespace_to_dict(namespace_two_obj)
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_one_obj,
            namespace_dict=namespace_one_dict,
            namespace_name=namespace_one
        )
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_two_obj,
            namespace_dict=namespace_two_dict,
            namespace_name=namespace_two
        )

    def test_list_schemas(
        self,
        batch,
        schematizer,
        topic_one_active,
        schema_one_active
    ):
        actual_schemas = batch.list_schemas(topic_one_active.name)
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        assert len(actual_schemas) == 1
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schemas[0]
        )

    def test_list_topics_source_id_no_sort(
        self,
        batch,
        topic_one_active,
        source_one_active,
        namespace_one
    ):
        actual_topics = batch.list_topics(
            source_id=topic_one_active.source.source_id
        )
        assert len(actual_topics) == 1
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=actual_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )

    def test_list_topics_source_id_sort(
        self,
        batch,
        topic_one_inactive,
        topic_one_inactive_b
    ):
        actual_topics = batch.list_topics(
            source_id=topic_one_inactive.source.source_id,
            sort_by='name',
            descending_order=True
        )
        # Can have higher length if the container is
        # not restarted in between runs
        assert len(actual_topics) >= 2
        a_name = topic_one_inactive.name
        b_name = topic_one_inactive_b.name
        a_pos = -1
        b_pos = -1
        for i, topic_dict in enumerate(actual_topics):
            if topic_dict['name'] == a_name:
                a_pos = i
            elif topic_dict['name'] == b_name:
                b_pos = i
        assert a_pos != -1
        assert b_pos != -1
        assert (
            (a_pos < b_pos and a_name > b_name) or
            (a_pos > b_pos and a_name < b_name)
        )

    def test_list_topics_namespace_source_names_no_sort(
        self,
        batch,
        source_one_active,
        namespace_one,
        topic_one_active
    ):
        actual_topics = batch.list_topics(
            namespace_name=namespace_one,
            source_name=source_one_active
        )
        assert len(actual_topics) == 1
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=actual_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )

    def test_list_sources_namespace_name_sort(
        self,
        batch,
        namespace_one,
        topic_one_active,
        topic_one_inactive,
        source_one_active,
        source_one_inactive
    ):
        actual_sources = batch.list_sources(
            namespace_name=namespace_one,
            sort_by='name'
        )
        active_source_obj = topic_one_active.source
        inactive_source_obj = topic_one_inactive.source
        assert len(actual_sources) == 2
        self._assert_source_equals_source_dict(
            source=active_source_obj,
            source_dict=actual_sources[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            active_topic_count=1
        )
        self._assert_source_equals_source_dict(
            source=inactive_source_obj,
            source_dict=actual_sources[1],
            namespace_name=namespace_one,
            source_name=source_one_inactive,
            active_topic_count=0
        )

    def test_list_sources_no_name_no_sort(
        self,
        batch,
        sources
    ):
        actual_sources = batch.list_sources()
        found_sources = [False for source in sources]
        for actual_source in actual_sources:
            source_name = actual_source['name']
            for i, expected_source in enumerate(sources):
                if expected_source == source_name:
                    assert not found_sources[i]
                    found_sources[i] = True
                    break
        for i, found_result in enumerate(found_sources):
            if not found_result:
                assert not "Could not find {} when listing all namespaces".format(sources[i])

    def test_list_namespaces(
        self,
        batch,
        namespace_one,
        namespace_two,
        topic_one_active,
        topic_two_active
    ):
        namespace_one_obj = topic_one_active.source.namespace
        namespace_two_obj = topic_two_active.source.namespace
        actual_namespaces = batch.list_namespaces(
            sort_by='name'
        )
        one_pos = -1
        two_pos = -1
        for i, namespace_dict in enumerate(actual_namespaces):
            namespace_name = namespace_dict['name']
            if namespace_name == namespace_one:
                assert one_pos == -1
                one_pos = i
                self._assert_namespace_equals_namespace_dict(
                    namespace=namespace_one_obj,
                    namespace_name=namespace_one,
                    namespace_dict=namespace_dict
                )
            elif namespace_name == namespace_two:
                assert two_pos == -1
                two_pos = i
                self._assert_namespace_equals_namespace_dict(
                    namespace=namespace_two_obj,
                    namespace_name=namespace_two,
                    namespace_dict=namespace_dict
                )
        assert one_pos != -1
        assert two_pos != -1
        assert one_pos < two_pos

    def test_info_topic(
        self,
        batch,
        schematizer,
        schema_one_active,
        namespace_one,
        source_one_active,
        topic_one_active
    ):
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        topic_dict = batch.info_topic(topic_one_active.name)
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=topic_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )
        actual_schemas = topic_dict['schemas']
        assert len(actual_schemas) == 1
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schemas[0]
        )

    def test_info_source_id(
        self,
        batch,
        topic_one_inactive,
        topic_one_inactive_b,
        source_one_inactive,
        namespace_one
    ):
        source_obj = topic_one_inactive.source
        source_dict = batch.info_source(
            source_id=source_obj.source_id,
            source_name=None,
            namespace_name=None
        )
        self._assert_source_equals_source_dict(
            source=source_obj,
            source_dict=source_dict,
            namespace_name=namespace_one,
            source_name=source_one_inactive,
            active_topic_count=0
        )
        source_topics = source_dict['topics']
        assert len(source_topics) == 2
        try:
            self._assert_topic_equals_topic_dict(
                topic=topic_one_inactive,
                topic_dict=source_topics[0],
                namespace_name=namespace_one,
                source_name=source_one_inactive,
                is_active=False
            )
            self._assert_topic_equals_topic_dict(
                topic=topic_one_inactive_b,
                topic_dict=source_topics[1],
                namespace_name=namespace_one,
                source_name=source_one_inactive,
                is_active=False
            )
        except AssertionError:
            self._assert_topic_equals_topic_dict(
                topic=topic_one_inactive,
                topic_dict=source_topics[1],
                namespace_name=namespace_one,
                source_name=source_one_inactive,
                is_active=False
            )
            self._assert_topic_equals_topic_dict(
                topic=topic_one_inactive_b,
                topic_dict=source_topics[0],
                namespace_name=namespace_one,
                source_name=source_one_inactive,
                is_active=False
            )

    def test_info_source_missing_source_name(
        self,
        batch,
        namespace_one
    ):
        with pytest.raises(ValueError) as e:
            batch.info_source(
                source_id=None,
                source_name="this_source_will_not_exist",
                namespace_name=namespace_one
            )
        assert e.value.args
        assert "Given SOURCE_NAME|NAMESPACE_NAME doesn't exist" in e.value.args[0]

    def test_info_source_name_pair(
        self,
        batch,
        topic_one_active,
        source_one_active,
        namespace_one
    ):
        source_dict = batch.info_source(
            source_id=None,
            source_name=source_one_active,
            namespace_name=namespace_one
        )
        source_obj = topic_one_active.source
        self._assert_source_equals_source_dict(
            source=source_obj,
            source_dict=source_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            active_topic_count=1
        )
        source_topics = source_dict['topics']
        assert len(source_topics) == 1
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=source_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )

    def test_info_namespace_missing(
        self,
        batch
    ):
        with pytest.raises(ValueError) as e:
            batch.info_namespace("not_a_namespace")
        assert e.value.args
        assert "Given namespace doesn't exist" in e.value.args[0]

    def test_info_namespace(
        self,
        batch,
        namespace_two,
        source_two_inactive,
        source_two_active,
        topic_two_active
    ):
        namespace_obj = topic_two_active.source.namespace
        namespace_dict = batch.info_namespace(namespace_two)
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_obj,
            namespace_dict=namespace_dict,
            namespace_name=namespace_two
        )
        namespace_sources = namespace_dict['sources']
        assert len(namespace_sources) == 2
        if namespace_sources[0]['name'] == source_two_active:
            assert namespace_sources[1]['name'] == source_two_inactive
        else:
            assert namespace_sources[0]['name'] == source_two_inactive
            assert namespace_sources[1]['name'] == source_two_active

    def _assert_schema_equals_schema_dict(
        self,
        topic_schema,
        schema_obj,
        schema_dict
    ):
        assert topic_schema.schema_id == schema_dict['schema_id']
        assert topic_schema.base_schema_id == schema_dict['base_schema_id']
        assert topic_schema.primary_keys == schema_dict['primary_keys']
        assert topic_schema.note == schema_dict['note']
        assert simplejson.loads(schema_obj.schema) == schema_dict['schema_json']
        assert schema_obj.status == schema_dict['status']

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
