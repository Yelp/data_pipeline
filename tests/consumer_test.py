# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing
import time

import mock
import pytest

from data_pipeline._avro_util import AvroStringWriter
from data_pipeline._avro_util import generate_payload_data
from data_pipeline.consumer import ConsumerTopicState
from data_pipeline.consumer import KafkaConsumer
from data_pipeline.consumer import MultiprocessingConsumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import Message
from data_pipeline.message import UpdateMessage
from data_pipeline.producer import Producer
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


@pytest.mark.usefixtures("patch_dry_run", "configure_teams")
class BaseConsumerTest(object):
    @pytest.yield_fixture
    def patch_dry_run(self):
        with mock.patch.object(
            Message,
            'dry_run',
            new_callable=mock.PropertyMock
        ) as mock_dry_run:
            mock_dry_run.return_value = True
            yield mock_dry_run

    @property
    def test_buffer_size(self):
        return 5

    @pytest.fixture
    def producer_name(self):
        return 'producer_1'

    @pytest.fixture()
    def producer_instance(self, kafka_docker, producer_name, team_name):
        return Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False
        )

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.yield_fixture
    def consumer(self, consumer_instance):
        with consumer_instance as consumer:
            yield consumer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture
    def consumer_asserter(self, consumer, message):
        return ConsumerAsserter(consumer=consumer, expected_msg=message)

    @pytest.fixture(scope='module')
    def topic(self, topic_name, kafka_docker):
        create_kafka_docker_topic(kafka_docker, topic_name)
        return topic_name

    @pytest.fixture(scope='module')
    def monitor(self, kafka_docker):
        create_kafka_docker_topic(kafka_docker, str('message-monitoring-log'))
        return str('message-monitoring-log')

    def test_get_message_none(self, consumer, topic):
        message = consumer.get_message(blocking=True, timeout=TIMEOUT)
        assert message is None
        assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_basic_iteration(
            self,
            publish_messages,
            consumer_asserter,
            monitor
    ):
        publish_messages(1)
        for msg in consumer_asserter.consumer:
            with consumer_asserter.consumer.ensure_committed(msg):
                consumer_asserter.assert_messages(
                    [msg],
                    expect_buffer_empty=True
                )
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
            timestamp=1500
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
            expected_msg_count=1,
            expect_buffer_empty=True
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
            consumer_asserter.assert_messages(
                [msg],
                expect_buffer_empty=True
            )

    def test_consume_using_get_messages(
            self,
            publish_messages,
            consumer_asserter
    ):
        publish_messages(2)
        consumer_asserter.get_and_assert_messages(
            count=2,
            expected_msg_count=2,
            expect_buffer_empty=True
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
            expected_msg_count=2,
            expect_buffer_empty=True
        )

        # Verify that we are not going to get any new messages
        consumer_asserter.get_and_assert_messages(
            count=10,
            expected_msg_count=0,
            expect_buffer_empty=True
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
            expected_msg_count=2,
            expect_buffer_empty=True
        )


class TestMultiprocessingConsumer(BaseConsumerTest):
    @pytest.fixture
    def publish_messages(self, producer, message, consumer):
        def _publish_messages(count):
            assert count > 0
            for _ in xrange(count):
                producer.publish(message)
            producer.flush()
            # wait until the consumer has retrieved a message before returning
            while consumer.message_buffer.empty():
                time.sleep(TIMEOUT)
        return _publish_messages

    @pytest.fixture(params=[
        {'decode_payload_in_workers': False},
        {'decode_payload_in_workers': True},
    ])
    def consumer_instance(self, request, topic, kafka_docker, team_name):
        return MultiprocessingConsumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None},
            max_buffer_size=self.test_buffer_size,
            decode_payload_in_workers=request.param['decode_payload_in_workers']
        )

    def test_get_messages_empty(self, consumer, topic,):
        messages = consumer.get_messages(count=10, blocking=True, timeout=TIMEOUT)
        assert len(messages) == 0
        assert consumer.message_buffer.empty()
        assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_maximum_buffer_size(
            self,
            publish_messages,
            consumer_asserter,
            monitor
    ):
        published_count = self.test_buffer_size + 1
        publish_messages(published_count)

        # Introduce a wait since we will not be using a blocking get_messages
        # and the consumer sub-processes will need time to fill the buffer
        while not consumer_asserter.consumer.message_buffer.full():
            time.sleep(TIMEOUT)

        msgs = consumer_asserter.get_and_assert_messages(
            count=published_count,
            expected_msg_count=self.test_buffer_size,
            blocking=False  # drain the buffer, then return
        )

        # Finish getting the rest of the messages
        consumer_asserter.get_and_assert_messages(
            count=published_count,
            expected_msg_count=published_count - len(msgs),
            expect_buffer_empty=True
        )


class TestKafkaConsumer(BaseConsumerTest):

    @pytest.fixture
    def publish_messages(self, producer, message, consumer):
        def _publish_messages(count):
            assert count > 0
            for _ in xrange(count):
                producer.publish(message)
            producer.flush()
        return _publish_messages

    @pytest.fixture(params=[
        {'decode_payload_in_workers': False},
        {'decode_payload_in_workers': True},
    ])
    def consumer_instance(self, request, topic, kafka_docker, team_name):
        return KafkaConsumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None},
            decode_payload_in_workers=request.param['decode_payload_in_workers']
        )

    def test_get_messages_empty(self, consumer, topic,):
        messages = consumer.get_messages(count=10, blocking=True, timeout=TIMEOUT)
        assert len(messages) == 0
        assert consumer.topic_to_consumer_topic_state_map[topic] is None


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
            expect_buffer_empty=None,
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
            self.assert_messages(messages, expect_buffer_empty)
        return messages

    def assert_messages(self, actual_msgs, expect_buffer_empty=None):
        assert isinstance(actual_msgs, list)
        for actual_msg in actual_msgs:
            self.assert_single_message(actual_msg, self.expected_msg)
        if isinstance(self.consumer, MultiprocessingConsumer):
            self.assert_consumer_state(expect_buffer_empty)

    def assert_single_message(self, actual_msg, expected_msg):
        assert actual_msg.message_type == expected_msg.message_type
        assert actual_msg.payload == expected_msg.payload
        assert actual_msg.schema_id == expected_msg.schema_id
        assert actual_msg.topic == expected_msg.topic
        assert actual_msg.payload_data == expected_msg.payload_data
        if isinstance(expected_msg, UpdateMessage):
            assert actual_msg.previous_payload == expected_msg.previous_payload
            assert actual_msg.previous_payload_data == expected_msg.previous_payload_data

    def assert_consumer_state(self, expect_buffer_empty=None):
        consume_topic_state = self.consumer.topic_to_consumer_topic_state_map[
            self.expected_topic
        ]
        assert isinstance(consume_topic_state, ConsumerTopicState)
        assert consume_topic_state.last_seen_schema_id == self.expected_schema_id

        # We can either expect it to be empty, expect it not to be empty, or
        # if 'None' we can't have any expectations
        if expect_buffer_empty is not None:
            assert self.consumer.message_buffer.empty() == expect_buffer_empty
