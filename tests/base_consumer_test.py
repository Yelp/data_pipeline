# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import datetime
import random
import time

import mock
import pytest
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import \
    generate_payload_data

from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.base_consumer import TopicFilter
from data_pipeline.consumer_source import MultiTopics
from data_pipeline.consumer_source import NewTopicOnlyInDataTarget
from data_pipeline.consumer_source import NewTopicOnlyInNamespace
from data_pipeline.consumer_source import NewTopicOnlyInSource
from data_pipeline.consumer_source import SingleSchema
from data_pipeline.consumer_source import SingleTopic
from data_pipeline.consumer_source import TopicInDataTarget
from data_pipeline.consumer_source import TopicInNamespace
from data_pipeline.consumer_source import TopicInSource
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import UpdateMessage
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.models.data_source_type_enum \
    import DataSourceTypeEnum
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


TIMEOUT = 1.0
""" TIMEOUT is used for all 'get_messages' calls in these tests. It's
essential that this value is large enough for the background workers
to have a chance to retrieve the messages, but otherwise as small
as possible as this has a direct impact on time it takes to execute
the tests.

Unfortunately these tests can flake if the consumers happen to take
too long to retrieve/decode the messages from kafka and miss the timeout
window and there is currently no mechanism to know if a consumer has
attempted and failed to retrieve a message we expect it to retrieve, vs
just taking longer than expected.

TODO(DATAPIPE-249|joshszep): Make data_pipeline clientlib Consumer tests
faster and flake-proof
"""


@pytest.mark.usefixtures("configure_teams")
class BaseConsumerTest(object):
    @pytest.fixture
    def producer_name(self):
        return 'producer_1'

    @pytest.fixture
    def producer_instance(self, containers, producer_name, team_name):
        instance = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False
        )
        containers.create_kafka_topic(instance.monitor.monitor_topic)
        return instance

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer

    @pytest.yield_fixture
    def consumer(self, consumer_instance):
        with consumer_instance as consumer:
            yield consumer

    @pytest.fixture
    def consumer_asserter(self, consumer, message):
        return ConsumerAsserter(consumer=consumer, expected_msg=message)

    @pytest.fixture(scope='module')
    def topic(self, containers, topic_name):
        containers.create_kafka_topic(topic_name)
        return topic_name

    def test_get_message_none(self, consumer, topic):
        message = consumer.get_message(blocking=True, timeout=TIMEOUT)
        assert message is None
        assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_basic_iteration(
            self,
            publish_messages,
            consumer_asserter
    ):
        publish_messages(1)
        for msg in consumer_asserter.consumer:
            with consumer_asserter.consumer.ensure_committed(msg):
                consumer_asserter.assert_messages([msg])
            break

    @pytest.fixture
    def example_prev_payload_data(self, example_schema_obj):
        return generate_payload_data(example_schema_obj)

    @pytest.fixture
    def previous_payload(self, example_schema_obj, example_prev_payload_data):
        return AvroStringWriter(
            example_schema_obj
        ).encode(
            example_prev_payload_data
        )

    @pytest.fixture
    def update_message(self, topic, payload, previous_payload, registered_schema,
                       example_payload_data, example_prev_payload_data):
        msg = UpdateMessage(
            topic=topic,
            schema_id=registered_schema.schema_id,
            payload=payload,
            previous_payload=previous_payload,
            timestamp=1500,
            contains_pii=False
        )
        # TODO [DATAPIPE-249|clin] as part of refactoring and cleanup consumer
        # tests, let's re-visit and see if these assertions are needed.
        assert msg.topic == topic
        assert msg.schema_id == registered_schema.schema_id
        assert msg.payload == payload
        assert msg.payload_data == example_payload_data
        assert msg.previous_payload == previous_payload
        assert msg.previous_payload_data == example_prev_payload_data
        return msg

    def test_consume_update_message(self, producer, consumer, update_message):
        producer.publish(update_message)
        producer.flush()

        consumer_asserter = ConsumerAsserter(
            consumer=consumer,
            expected_msg=update_message
        )
        consumer_asserter.get_and_assert_messages(
            count=1,
            expected_msg_count=1
        )

    def test_consume_using_get_message(
            self,
            publish_messages,
            consumer_asserter
    ):
        publish_messages(1)
        consumer = consumer_asserter.consumer
        with consumer.ensure_committed(
                consumer.get_message(blocking=True, timeout=TIMEOUT)
        ) as msg:
            consumer_asserter.assert_messages([msg])

    def test_consume_using_get_messages(
            self,
            publish_messages,
            consumer_asserter
    ):
        publish_messages(2)
        consumer_asserter.get_and_assert_messages(
            count=2,
            expected_msg_count=2
        )

    def test_basic_publish_retrieve_then_reset(
            self,
            publish_messages,
            consumer_asserter,
            topic
    ):
        publish_messages(2)

        # Get messages so that the topic_to_consumer_topic_state_map will
        # have a ConsumerTopicState for our topic
        consumer_asserter.get_and_assert_messages(
            count=2,
            expected_msg_count=2
        )

        # Verify that we are not going to get any new messages
        consumer_asserter.get_and_assert_messages(
            count=10,
            expected_msg_count=0
        )

        # Set the offset to one previous so we can use reset_topics to
        # receive the same two messages again
        consumer = consumer_asserter.consumer
        topic_map = consumer.topic_to_consumer_topic_state_map
        topic_map[topic].partition_offset_map[0] -= 1
        consumer.reset_topics(topic_to_consumer_topic_state_map=topic_map)

        # Verify that we do get the same two messages again
        consumer_asserter.get_and_assert_messages(
            count=10,
            expected_msg_count=2
        )


class ConsumerAsserter(object):
    """ Helper class to encapsulate the common assertions in the consumer tests
    """

    def __init__(self, consumer, expected_msg):
        self.consumer = consumer
        self.expected_msg = expected_msg
        self.expected_topic = expected_msg.topic
        self.expected_schema_id = expected_msg.schema_id

    def get_and_assert_messages(
            self,
            count,
            expected_msg_count,
            blocking=True
    ):
        with self.consumer.ensure_committed(
            self.consumer.get_messages(
                count=count,
                blocking=blocking,
                timeout=TIMEOUT
            )
        ) as messages:
            assert len(messages) == expected_msg_count
            self.assert_messages(messages)
        return messages

    def assert_messages(self, actual_msgs):
        assert isinstance(actual_msgs, list)
        for actual_msg in actual_msgs:
            self.assert_single_message(actual_msg, self.expected_msg)
            self.assert_consumer_state()

    def assert_single_message(self, actual_msg, expected_msg):
        assert actual_msg.message_type == expected_msg.message_type
        assert actual_msg.payload == expected_msg.payload
        assert actual_msg.schema_id == expected_msg.schema_id
        assert actual_msg.topic == expected_msg.topic
        assert actual_msg.payload_data == expected_msg.payload_data
        if isinstance(expected_msg, UpdateMessage):
            assert actual_msg.previous_payload == expected_msg.previous_payload
            assert actual_msg.previous_payload_data == expected_msg.previous_payload_data

    def assert_consumer_state(self):
        consumer_topic_state = self.consumer.topic_to_consumer_topic_state_map[
            self.expected_topic
        ]
        assert isinstance(consumer_topic_state, ConsumerTopicState)
        assert consumer_topic_state.last_seen_schema_id == self.expected_schema_id


@pytest.mark.usefixtures("configure_teams")
class RefreshNewTopicsTest(object):

    @pytest.fixture
    def yelp_namespace(self):
        return 'yelp_{0}'.format(random.random())

    @pytest.fixture
    def biz_src(self):
        return 'biz_{0}'.format(random.random())

    @pytest.fixture
    def biz_schema(self, yelp_namespace, biz_src, containers):
        return self._register_schema(yelp_namespace, biz_src, containers)

    @pytest.fixture
    def usr_src(self):
        return 'user_{0}'.format(random.random())

    @pytest.fixture
    def usr_schema(self, yelp_namespace, usr_src, containers):
        return self._register_schema(yelp_namespace, usr_src, containers)

    @pytest.fixture
    def aux_namespace(self):
        return 'aux_{0}'.format(random.random())

    @pytest.fixture
    def cta_src(self):
        return 'cta_{0}'.format(random.random())

    @pytest.fixture(autouse=True)
    def cta_schema(self, aux_namespace, cta_src, containers):
        return self._register_schema(aux_namespace, cta_src, containers)

    @pytest.fixture
    def pre_rebalance_callback(self):
        return mock.Mock()

    @pytest.fixture
    def post_rebalance_callback(self):
        return mock.Mock()

    def _register_schema(self, namespace, source, containers):
        avro_schema = {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'fields': [{'type': 'int', 'name': 'id'}]
        }
        reg_schema = get_schematizer().register_schema_from_schema_json(
            namespace=namespace,
            source=source,
            schema_json=avro_schema,
            source_owner_email='bam+test@yelp.com',
            contains_pii=False
        )
        containers.create_kafka_topic(str(reg_schema.topic.name))
        return reg_schema

    @pytest.fixture(scope='class')
    def test_schema(self, containers):
        return self._register_schema('test_namespace', 'test_src', containers)

    @pytest.fixture(scope='class')
    def topic(self, containers, test_schema):
        topic_name = str(test_schema.topic.name)
        containers.create_kafka_topic(topic_name)
        return topic_name

    @pytest.yield_fixture
    def consumer(self, consumer_instance):
        with consumer_instance as consumer:
            yield consumer

    def test_no_newer_topics(self, consumer, yelp_namespace, biz_schema):
        expected = self._get_expected_value(
            original_states=consumer.topic_to_consumer_topic_state_map
        )
        new_topics = consumer.refresh_new_topics(TopicFilter(
            namespace_name=yelp_namespace,
            created_after=self._increment_seconds(biz_schema.created_at, seconds=1)
        ))
        assert new_topics == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected
        )

    def test_refresh_newer_topics_in_yelp_namespace(
        self,
        consumer,
        yelp_namespace,
        biz_schema,
        usr_schema
    ):
        biz_topic = biz_schema.topic
        usr_topic = usr_schema.topic
        expected = self._get_expected_value(
            original_states=consumer.topic_to_consumer_topic_state_map,
            new_states={biz_topic.name: None, usr_topic.name: None}
        )

        new_topics = consumer.refresh_new_topics(TopicFilter(
            namespace_name=yelp_namespace,
            created_after=self._increment_seconds(
                min(biz_schema.created_at, usr_schema.created_at),
                seconds=-1
            )
        ))

        assert new_topics == [biz_topic, usr_topic]
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected
        )

    def test_already_tailed_topic_state_remains_after_refresh(
        self,
        consumer,
        test_schema,
    ):
        self._publish_then_consume_message(consumer, test_schema)

        topic = test_schema.topic
        expected = self._get_expected_value(
            original_states=consumer.topic_to_consumer_topic_state_map
        )

        new_topics = consumer.refresh_new_topics(TopicFilter(
            source_name=topic.source.name,
            created_after=self._increment_seconds(topic.created_at, seconds=-1)
        ))

        assert topic.topic_id not in [new_topic.topic_id for new_topic in new_topics]
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected
        )

    def _publish_then_consume_message(self, consumer, avro_schema):
        with Producer(
            'test_producer',
            team_name='bam',
            expected_frequency_seconds=ExpectedFrequency.constantly,
            monitoring_enabled=False
        ) as producer:
            message = UpdateMessage(
                topic=str(avro_schema.topic.name),
                schema_id=avro_schema.schema_id,
                payload_data={'id': 2},
                previous_payload_data={'id': 1}
            )
            producer.publish(message)
            producer.flush()

        consumer.get_messages(1, blocking=True, timeout=TIMEOUT)

    def test_refresh_with_custom_filter(
        self,
        consumer,
        yelp_namespace,
        biz_schema,
        usr_schema
    ):
        biz_topic = biz_schema.topic
        expected = self._get_expected_value(
            original_states=consumer.topic_to_consumer_topic_state_map,
            new_states={biz_topic.name: None}
        )

        new_topics = consumer.refresh_new_topics(TopicFilter(
            namespace_name=yelp_namespace,
            created_after=self._increment_seconds(
                min(biz_schema.created_at, usr_schema.created_at),
                seconds=-1
            ),
            filter_func=lambda topics: [biz_topic]
        ))

        assert new_topics == [biz_topic]
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected
        )

    def test_with_bad_namespace(self, consumer):
        actual = consumer.refresh_new_topics(TopicFilter(
            namespace_name='bad.namespace',
            created_after=0
        ))
        assert actual == []

    def test_with_bad_source(self, consumer, yelp_namespace):
        actual = consumer.refresh_new_topics(TopicFilter(
            namespace_name=yelp_namespace,
            source_name='bad.source',
            created_after=0
        ))
        assert actual == []

    def test_with_before_refresh_handler(
        self,
        consumer,
        yelp_namespace,
        biz_schema
    ):
        mock_handler = mock.Mock()
        new_topics = consumer.refresh_new_topics(
            TopicFilter(
                namespace_name=yelp_namespace,
                created_after=self._increment_seconds(
                    biz_schema.created_at,
                    seconds=-1
                )
            ),
            before_refresh_handler=mock_handler
        )
        biz_topic = biz_schema.topic
        assert new_topics == [biz_topic]
        mock_handler.assert_called_once_with([biz_topic])

    def _get_expected_value(self, original_states, new_states=None):
        expected = copy.deepcopy(original_states)
        expected.update(new_states or {})
        return expected

    def _get_utc_timestamp(self, dt):
        return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

    def _increment_seconds(self, dt, seconds):
        return self._get_utc_timestamp(dt + datetime.timedelta(seconds=seconds))

    def _assert_equal_state_map(self, actual_map, expected_map):
        assert set(actual_map.keys()) == set(expected_map.keys())
        for topic, actual in actual_map.iteritems():
            expected = expected_map[topic]
            if expected is None:
                assert actual is None
                continue
            assert actual.last_seen_schema_id == expected.last_seen_schema_id
            assert actual.partition_offset_map == expected.partition_offset_map


@pytest.mark.usefixtures("configure_teams")
class RefreshTopicsTestBase(object):

    @pytest.yield_fixture
    def consumer(self, consumer_instance):
        with consumer_instance as consumer:
            yield consumer

    @pytest.fixture(scope='module')
    def _register_schema(self, containers, schematizer_client):
        def register_func(namespace, source, avro_schema=None):
            avro_schema = avro_schema or {
                'type': 'record',
                'name': source,
                'namespace': namespace,
                'fields': [{'type': 'int', 'name': 'id'}]
            }
            new_schema = schematizer_client.register_schema_from_schema_json(
                namespace=namespace,
                source=source,
                schema_json=avro_schema,
                source_owner_email='bam+test@yelp.com',
                contains_pii=False
            )
            containers.create_kafka_topic(str(new_schema.topic.name))
            return new_schema
        return register_func

    def random_name(self, prefix=None):
        suffix = random.random()
        return '{}_{}'.format(prefix, suffix) if prefix else '{}'.format(suffix)

    @pytest.fixture
    def foo_namespace(self):
        return self.random_name('foo_ns')

    @pytest.fixture
    def foo_src(self):
        return self.random_name('foo_src')

    @pytest.fixture
    def foo_schema(self, foo_namespace, foo_src, _register_schema):
        return _register_schema(foo_namespace, foo_src)

    @pytest.fixture
    def foo_topic(self, foo_schema):
        return foo_schema.topic.name

    @property
    def target_type(self):
        return 'redshift'

    @property
    def destination(self):
        return 'dw.redshift.destination'

    @pytest.fixture
    def data_target(self, schematizer_client):
        return schematizer_client.create_data_target(
            target_type=self.target_type,
            destination=self.destination
        )

    @pytest.fixture
    def consumer_group(self, data_target, schematizer_client):
        return schematizer_client.create_consumer_group(
            group_name=self.random_name('test_group'),
            data_target_id=data_target.data_target_id
        )

    @pytest.fixture
    def data_source(self, foo_schema, consumer_group, schematizer_client):
        return schematizer_client.create_consumer_group_data_source(
            consumer_group_id=consumer_group.consumer_group_id,
            data_source_type=DataSourceTypeEnum.Source,
            data_source_id=foo_schema.topic.source.source_id
        )

    @pytest.fixture(scope='class')
    def test_schema(self, _register_schema):
        return _register_schema('test_namespace', 'test_src')

    @pytest.fixture(scope='class')
    def topic(self, test_schema):
        return str(test_schema.topic.name)

    @pytest.fixture
    def schema_with_bad_topic(self, schematizer_client, foo_namespace, foo_src):
        avro_schema = {
            'type': 'record',
            'name': foo_src,
            'namespace': foo_namespace,
            'fields': [{'type': 'int', 'name': 'id'}]
        }
        new_schema = schematizer_client.register_schema_from_schema_json(
            namespace=foo_namespace,
            source=foo_src,
            schema_json=avro_schema,
            source_owner_email='bam+test@yelp.com',
            contains_pii=False
        )
        return new_schema

    def _publish_then_consume_message(self, consumer, avro_schema):
        with Producer(
            'test_producer',
            team_name='bam',
            expected_frequency_seconds=ExpectedFrequency.constantly,
            monitoring_enabled=False
        ) as producer:
            message = UpdateMessage(
                topic=str(avro_schema.topic.name),
                schema_id=avro_schema.schema_id,
                payload_data={'id': 2},
                previous_payload_data={'id': 1}
            )
            producer.publish(message)
            producer.flush()

        consumer.get_messages(1, blocking=True, timeout=TIMEOUT)

    def _assert_equal_state_map(self, actual_map, expected_map):
        assert set(actual_map.keys()) == set(expected_map.keys())
        for topic, actual in actual_map.iteritems():
            expected = expected_map[topic]
            if expected is None:
                assert actual is None
                continue
            assert actual.last_seen_schema_id == expected.last_seen_schema_id
            assert actual.partition_offset_map == expected.partition_offset_map


class RefreshFixedTopicTests(RefreshTopicsTestBase):

    def test_get_toipcs(self, consumer, consumer_source, expected_topics, topic):
        expected_map = {topic: None}
        expected_map.update({topic: None for topic in expected_topics})

        actual = consumer.refresh_topics(consumer_source)

        assert set(actual) == set(expected_topics)
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected_map
        )

    def test_get_topics_multiple_times(
        self,
        consumer,
        consumer_source,
        expected_topics,
        topic
    ):
        expected_map = {topic: None}
        expected_map.update({topic: None for topic in expected_topics})

        actual = consumer.refresh_topics(consumer_source)
        assert set(actual) == set(expected_topics)
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected_map
        )

        actual = consumer.refresh_topics(consumer_source)
        assert actual == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map=expected_map
        )

    def test_bad_topic(self, consumer, bad_consumer_source, topic):
        actual = consumer.refresh_topics(bad_consumer_source)

        assert actual == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map={topic: None}
        )


class SingleTopicSetupMixin(RefreshFixedTopicTests):

    @pytest.fixture
    def expected_topics(self, foo_topic):
        return [foo_topic]

    @pytest.fixture
    def consumer_source(self, foo_topic):
        return SingleTopic(topic_name=foo_topic)

    @pytest.fixture
    def bad_consumer_source(self):
        return SingleTopic(topic_name='bad_topic')


class MultiTopicsSetupMixin(RefreshFixedTopicTests):

    @pytest.fixture
    def bar_schema(self, foo_namespace, foo_src, _register_schema):
        new_schema = {
            'type': 'record',
            'name': foo_src,
            'namespace': foo_namespace,
            'fields': [{'type': 'string', 'name': 'memo'}]
        }
        return _register_schema(foo_namespace, foo_src, new_schema)

    @pytest.fixture
    def bar_topic(self, bar_schema):
        return bar_schema.topic.name

    @pytest.fixture
    def expected_topics(self, foo_topic, bar_topic):
        return [foo_topic, bar_topic]

    @pytest.fixture
    def consumer_source(self, foo_topic, bar_topic):
        return MultiTopics(foo_topic, bar_topic)

    @pytest.fixture
    def bad_consumer_source(self):
        return MultiTopics('bad_topic_1', 'bad_topic_2')


class SingleSchemaSetupMixin(RefreshFixedTopicTests):

    @pytest.fixture
    def expected_topics(self, foo_schema):
        return [foo_schema.topic.name]

    @pytest.fixture
    def consumer_source(self, foo_schema):
        return SingleSchema(schema_id=foo_schema.schema_id)

    @pytest.fixture
    def bad_consumer_source(self, schema_with_bad_topic):
        return SingleSchema(schema_id=schema_with_bad_topic.schema_id)


class RefreshDynamicTopicTests(RefreshTopicsTestBase):

    def test_no_topics_in_consumer_source(self, consumer, consumer_source, topic):
        actual = consumer.refresh_topics(consumer_source)
        assert actual == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map={topic: None}
        )

    def test_pick_up_new_topic(
        self,
        consumer,
        consumer_source,
        foo_topic,
        data_source,
        test_schema,
        topic
    ):
        assert consumer.topic_to_consumer_topic_state_map == {topic: None}

        self._publish_then_consume_message(consumer, test_schema)
        new_topic_state = consumer.topic_to_consumer_topic_state_map[topic]
        assert new_topic_state is not None

        actual = consumer.refresh_topics(consumer_source)

        assert actual == [foo_topic]
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map={
                topic: new_topic_state,
                foo_topic: None
            }
        )

    def test_not_pick_up_new_topic_in_diff_source(
        self,
        consumer,
        consumer_source,
        foo_topic,
        data_source,
        topic,
        _register_schema
    ):
        actual = consumer.refresh_topics(consumer_source)
        assert actual == [foo_topic]

        # force topic is created at least 1 second after last topic query.
        time.sleep(1)
        new_schema = {
            'type': 'record',
            'name': 'src_two',
            'namespace': 'namespace_two',
            'fields': [{'type': 'bytes', 'name': 'md5'}]
        }
        _register_schema('namespace_two', 'src_two', new_schema)

        actual = consumer.refresh_topics(consumer_source)
        assert actual == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map={topic: None, foo_topic: None}
        )

    def test_bad_consumer_source(self, consumer, bad_consumer_source, topic):
        actual = consumer.refresh_topics(bad_consumer_source)

        assert actual == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map={topic: None}
        )

    def test_consumer_source_has_bad_topic(
        self,
        consumer,
        consumer_source_with_bad_topic,
        topic
    ):
        actual = consumer.refresh_topics(consumer_source_with_bad_topic)

        assert actual == []
        self._assert_equal_state_map(
            actual_map=consumer.topic_to_consumer_topic_state_map,
            expected_map={topic: None}
        )


class TopicInNamespaceSetupMixin(RefreshDynamicTopicTests):

    @pytest.fixture(params=[TopicInNamespace, NewTopicOnlyInNamespace])
    def consumer_source_cls(self, request):
        return request.param

    @pytest.fixture
    def consumer_source(self, consumer_source_cls, foo_namespace):
        return consumer_source_cls(namespace_name=foo_namespace)

    @pytest.fixture
    def bad_consumer_source(self, consumer_source_cls):
        return consumer_source_cls(namespace_name='bad_namespace')

    @pytest.fixture
    def consumer_source_with_bad_topic(
        self,
        consumer_source_cls,
        schema_with_bad_topic
    ):
        return consumer_source_cls(
            namespace_name=schema_with_bad_topic.topic.source.namespace.name
        )


class TopicInSourceSetupMixin(RefreshDynamicTopicTests):

    @pytest.fixture(params=[TopicInSource, NewTopicOnlyInSource])
    def consumer_source_cls(self, request):
        return request.param

    @pytest.fixture
    def consumer_source(self, consumer_source_cls, foo_namespace, foo_src):
        return consumer_source_cls(
            namespace_name=foo_namespace,
            source_name=foo_src
        )

    @pytest.fixture
    def bad_consumer_source(self, consumer_source_cls):
        return consumer_source_cls(
            namespace_name='bad_namespace',
            source_name='bad_source'
        )

    @pytest.fixture
    def consumer_source_with_bad_topic(
        self,
        consumer_source_cls,
        schema_with_bad_topic
    ):
        return consumer_source_cls(
            namespace_name=schema_with_bad_topic.topic.source.namespace.name,
            source_name=schema_with_bad_topic.topic.source.name
        )


class TopicInDataTargetSetupMixin(RefreshDynamicTopicTests):

    @pytest.fixture(params=[TopicInDataTarget, NewTopicOnlyInDataTarget])
    def consumer_source_cls(self, request):
        return request.param

    @pytest.fixture
    def consumer_source(self, consumer_source_cls, data_target):
        return consumer_source_cls(data_target_id=data_target.data_target_id)

    @pytest.fixture
    def bad_consumer_source(self, consumer_source_cls, schematizer_client):
        data_target = schematizer_client.create_data_target(
            target_type='bad target type',
            destination='bad destination'
        )
        return consumer_source_cls(data_target_id=data_target.data_target_id)

    @pytest.fixture
    def consumer_source_with_bad_topic(
        self,
        consumer_source_cls,
        schema_with_bad_topic,
        consumer_group,
        schematizer_client
    ):
        data_target = schematizer_client.create_data_target(
            target_type='some target type',
            destination='some destination'
        )
        schematizer_client.create_consumer_group_data_source(
            consumer_group_id=consumer_group.consumer_group_id,
            data_source_type=DataSourceTypeEnum.Source,
            data_source_id=schema_with_bad_topic.topic.source.source_id
        )
        return consumer_source_cls(data_target.data_target_id)
