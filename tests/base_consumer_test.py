# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import datetime
import random

import mock
import pytest

from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.base_consumer import TopicFilter
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import UpdateMessage
from data_pipeline.producer import Producer
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

    @pytest.yield_fixture
    def producer(self, team_name):
        with Producer(
            producer_name='producer_1',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False
        ) as producer:
            yield producer

    @pytest.fixture
    def publish_messages(self, producer):
        def _publish_messages(message, count):
            assert count > 0
            for _ in range(count):
                producer.publish(message)
            producer.flush()
        return _publish_messages

    @pytest.fixture(scope="module")
    def topic(self, registered_schema):
        return str(registered_schema.topic.name)

    @pytest.fixture(scope="module", autouse=True)
    def ensure_topic_exist(self, containers, topic):
        containers.create_kafka_topic(topic)

    def test_get_message_none(self, consumer_instance, topic):
        with consumer_instance as consumer:
            _messsage = consumer.get_message(blocking=True, timeout=TIMEOUT)
            assert _messsage is None
            assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_basic_iteration(self, consumer_instance, publish_messages, message):
        with consumer_instance as consumer:
            publish_messages(message, count=1)
            asserter = ConsumerAsserter(
                consumer=consumer,
                expected_message=message
            )
            for _message in consumer:
                asserter.assert_messages([_message], expected_count=1)
                break

    @pytest.fixture
    def update_message(self, payload, registered_schema):
        return UpdateMessage(
            schema_id=registered_schema.schema_id,
            payload=payload,
            previous_payload=payload
        )

    def test_get_update_message(
        self,
        consumer_instance,
        publish_messages,
        update_message
    ):
        with consumer_instance as consumer:
            publish_messages(update_message, count=1)
            asserter = ConsumerAsserter(
                consumer=consumer,
                expected_message=update_message
            )
            messages = consumer.get_messages(
                count=1,
                blocking=True,
                timeout=TIMEOUT
            )
            asserter.assert_messages(messages, expected_count=1)

    def test_get_message(self, consumer_instance, publish_messages, message):
        with consumer_instance as consumer:
            publish_messages(message, count=1)
            asserter = ConsumerAsserter(
                consumer=consumer,
                expected_message=message
            )
            _message = consumer.get_message(blocking=True, timeout=TIMEOUT)
            asserter.assert_messages([_message], expected_count=1)

    def test_get_messages(self, consumer_instance, publish_messages, message):
        with consumer_instance as consumer:
            publish_messages(message, count=2)
            asserter = ConsumerAsserter(
                consumer=consumer,
                expected_message=message
            )
            messages = consumer.get_messages(count=2, blocking=True, timeout=TIMEOUT)
            asserter.assert_messages(messages, expected_count=2)

    def test_get_messages_then_reset(
        self,
        consumer_instance,
        publish_messages,
        message
    ):
        with consumer_instance as consumer:
            publish_messages(message, count=2)
            asserter = ConsumerAsserter(
                consumer=consumer,
                expected_message=message
            )

            # Get messages so that the topic_to_consumer_topic_state_map will
            # have a ConsumerTopicState for the topic.  Getting more messages
            # than necessary is to verify only two published messages are consumed
            messages = consumer.get_messages(
                count=10,
                blocking=True,
                timeout=TIMEOUT
            )
            asserter.assert_messages(messages, expected_count=2)

            # Set the offset to one previous so we can use reset_topics to
            # receive the same two messages again
            topic_map = consumer.topic_to_consumer_topic_state_map
            topic_map[message.topic].partition_offset_map[0] -= 1
            consumer.reset_topics(topic_to_consumer_topic_state_map=topic_map)

            # Verify that we do get the same two messages again
            messages = consumer.get_messages(
                count=10,
                blocking=True,
                timeout=TIMEOUT
            )
            asserter.assert_messages(messages, expected_count=2)


class ConsumerAsserter(object):
    """ Helper class to encapsulate the common assertions in the consumer tests
    """

    def __init__(self, consumer, expected_message):
        self.consumer = consumer
        self.expected_message = expected_message
        self.expected_topic = expected_message.topic
        self.expected_schema_id = expected_message.schema_id

    def get_and_assert_messages(self, count, expected_msg_count, blocking=True):
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

    def assert_messages(self, actual_messages, expected_count):
        assert isinstance(actual_messages, list)
        assert len(actual_messages) == expected_count
        for actual_message in actual_messages:
            self.assert_equal_message(actual_message, self.expected_message)
        self.assert_consumer_state()

    def assert_equal_message(self, actual_message, expected_message):
        assert actual_message.message_type == expected_message.message_type
        assert actual_message.payload == expected_message.payload
        assert actual_message.schema_id == expected_message.schema_id
        assert actual_message.topic == expected_message.topic
        assert actual_message.payload_data == expected_message.payload_data

        if isinstance(expected_message, UpdateMessage):
            assert actual_message.previous_payload == \
                expected_message.previous_payload
            assert actual_message.previous_payload_data == \
                expected_message.previous_payload_data

    def assert_consumer_state(self):
        consumer_topic_state = self.consumer.topic_to_consumer_topic_state_map[
            self.expected_topic
        ]
        assert isinstance(consumer_topic_state, ConsumerTopicState)
        assert consumer_topic_state.last_seen_schema_id == self.expected_schema_id


@pytest.mark.usefixtures("configure_teams")
class RefreshTopicsTest(object):

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
