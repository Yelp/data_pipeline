# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import datetime
import random

import mock
import pytest
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import \
    generate_payload_data

from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.base_consumer import TopicFilter
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import UpdateMessage
from data_pipeline.producer import Producer
from data_pipeline.schema_cache import get_schematizer_client
from data_pipeline.testing_helpers.kafka_docker import create_kafka_docker_topic


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
    def producer_instance(self, kafka_docker, producer_name, team_name):
        instance = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False
        )
        create_kafka_docker_topic(kafka_docker, instance.monitor.monitor_topic)
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
    def topic(self, topic_name, kafka_docker):
        create_kafka_docker_topic(kafka_docker, topic_name)
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
class RefreshTopicsTest(object):

    @pytest.yield_fixture(scope='class')
    def schematizer(self):
        yield get_schematizer_client()

    @pytest.fixture
    def yelp_namespace(self):
        return 'yelp_{0}'.format(random.random())

    @pytest.fixture
    def biz_src(self):
        return 'biz_{0}'.format(random.random())

    @pytest.fixture
    def biz_schema(self, schematizer, yelp_namespace, biz_src):
        return self._register_schema(schematizer, yelp_namespace, biz_src)

    @pytest.fixture
    def usr_src(self):
        return 'user_{0}'.format(random.random())

    @pytest.fixture
    def usr_schema(self, schematizer, yelp_namespace, usr_src):
        return self._register_schema(schematizer, yelp_namespace, usr_src)

    @pytest.fixture
    def aux_namespace(self):
        return 'aux_{0}'.format(random.random())

    @pytest.fixture
    def cta_src(self):
        return 'cta_{0}'.format(random.random())

    @pytest.fixture(autouse=True)
    def cta_schema(self, schematizer, aux_namespace, cta_src):
        return self._register_schema(schematizer, aux_namespace, cta_src)

    def _register_schema(self, schematizer_client, namespace, source):
        avro_schema = {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'fields': [{'type': 'int', 'name': 'id'}]
        }
        return schematizer_client.register_schema_by_schema_json(
            namespace=namespace,
            source=source,
            schema_json=avro_schema,
            owner_email='bam+test@yelp.com',
            contains_pii=False
        )

    @pytest.fixture(scope='class')
    def test_schema(self, schematizer):
        return self._register_schema(schematizer, 'test_namespace', 'test_src')

    @pytest.fixture(scope='class')
    def topic(self, test_schema, kafka_docker):
        topic_name = str(test_schema.topic.name)
        create_kafka_docker_topic(kafka_docker, topic_name)
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
        usr_schema,
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
