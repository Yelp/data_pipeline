# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing

import pytest

from data_pipeline.consumer import Consumer
from data_pipeline.consumer import ConsumerTopicState
from data_pipeline.message import Message
from data_pipeline.producer import Producer
from tests.helpers.kafka_docker import create_kafka_docker_topic


class TestConsumer(object):

    test_timeout = 1.0
    test_buffer_size = 5

    @pytest.fixture()
    def producer_instance(self, kafka_docker):
        return Producer(use_work_pool=False)

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(params=[
        {'decode_payload_in_workers': False},
        {'decode_payload_in_workers': True},
    ])
    def consumer_instance(self, request, topic, kafka_docker):
        return Consumer(
            consumer_name='test_consumer',
            topic_to_consumer_topic_state_map={topic: None},
            max_buffer_size=self.test_buffer_size,
            decode_payload_in_workers=request.param['decode_payload_in_workers']
        )

    @pytest.yield_fixture
    def consumer(self, consumer_instance):
        with consumer_instance as consumer:
            yield consumer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope='module')
    def topic(self, topic_name, kafka_docker):
        create_kafka_docker_topic(kafka_docker, topic_name)
        return topic_name

    def test_get_message_none(self, consumer, topic):
        message = consumer.get_message(blocking=True, timeout=self.test_timeout)
        assert message is None
        assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_get_messages_empty(self, consumer, topic):
        messages = consumer.get_messages(count=10, blocking=True, timeout=self.test_timeout)
        assert len(messages) == 0
        assert consumer.message_buffer.empty()
        assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_basic_iteration(
            self,
            topic,
            message,
            producer,
            consumer_instance,
            example_payload_data,
            registered_schema
    ):
        self._publish_messages(producer, message, 1)
        with consumer_instance as consumer:
            for msg in consumer:
                if msg is None:
                    continue
                with consumer.ensure_committed(msg):
                    assert consumer.message_buffer.empty()
                    assert msg is not None
                    self._assert_consumer_state(
                        consumer=consumer,
                        actual_msgs=[msg],
                        expected_msg=message,
                        expected_topic=topic,
                        expected_schema_id=registered_schema.schema_id,
                        expected_payload_data=example_payload_data
                    )
                break

    def test_consume_using_get_message(
            self,
            topic,
            message,
            producer,
            consumer_instance,
            example_payload_data,
            registered_schema
    ):
        self._publish_messages(producer, message, 1)
        with consumer_instance as consumer:
            with consumer.ensure_committed(
                    consumer.get_message(blocking=True, timeout=self.test_timeout)
            ) as msg:
                assert consumer.message_buffer.empty()
                assert msg is not None
                self._assert_consumer_state(
                    consumer=consumer,
                    actual_msgs=[msg],
                    expected_msg=message,
                    expected_topic=topic,
                    expected_schema_id=registered_schema.schema_id,
                    expected_payload_data=example_payload_data
                )

    def test_consume_using_get_messages(
            self,
            message,
            producer,
            consumer_instance,
            registered_schema,
            topic,
            example_payload_data
    ):
        self._publish_messages(producer, message, 2)
        with consumer_instance as consumer:
            with consumer.ensure_committed(
                    consumer.get_messages(count=2, blocking=True, timeout=self.test_timeout)
            ) as messages:
                assert len(messages) == 2
                assert consumer.message_buffer.empty()
                self._assert_consumer_state(
                    consumer=consumer,
                    actual_msgs=messages,
                    expected_msg=message,
                    expected_topic=topic,
                    expected_schema_id=registered_schema.schema_id,
                    expected_payload_data=example_payload_data
                )

    def test_basic_publish_retrieve_then_reset(
            self,
            message,
            producer,
            consumer_instance,
            registered_schema,
            topic,
            example_payload_data
    ):
        self._publish_messages(producer, message, 2)
        with consumer_instance as consumer:
            with consumer.ensure_committed(
                    consumer.get_messages(count=10, blocking=True, timeout=self.test_timeout)
            ) as messages1:
                assert len(messages1) == 2
                assert consumer.message_buffer.empty()
                self._assert_consumer_state(
                    consumer=consumer,
                    actual_msgs=messages1,
                    expected_msg=message,
                    expected_topic=topic,
                    expected_schema_id=registered_schema.schema_id,
                    expected_payload_data=example_payload_data
                )

            # Verify that we are not going to get any new messages

            with consumer.ensure_committed(
                consumer.get_messages(count=10, blocking=True, timeout=self.test_timeout)
            ) as messages2:
                assert len(messages2) == 0
                assert consumer.message_buffer.empty()

            # Set the offset to one previous so after we reset_topics we can
            # expect to receive two messages again
            topic_map = consumer.topic_to_consumer_topic_state_map
            topic_map[topic].partition_offset_map[0] -= 1
            consumer.reset_topics(topic_to_consumer_topic_state_map=topic_map)
            with consumer.ensure_committed(
                consumer.get_messages(count=10, blocking=True, timeout=self.test_timeout)
            ) as messages3:
                assert len(messages3) == 2
                assert consumer.message_buffer.empty()
                self._assert_consumer_state(
                    consumer=consumer,
                    actual_msgs=messages3,
                    expected_msg=message,
                    expected_topic=topic,
                    expected_schema_id=registered_schema.schema_id,
                    expected_payload_data=example_payload_data
                )

    def test_maximum_buffer_size(
            self,
            topic,
            message,
            producer,
            consumer_instance,
            example_payload_data,
            registered_schema
    ):
        self._publish_messages(producer, message, self.test_buffer_size * 2)
        with consumer_instance as consumer:
            msgs = consumer.get_messages(
                count=self.test_buffer_size * 2,
                blocking=True,
                timeout=self.test_timeout
            )
            while len(msgs) < self.test_buffer_size * 2:
                new_msgs = consumer.get_messages(
                    count=self.test_buffer_size * 2,
                    blocking=True,
                    timeout=self.test_timeout
                )
                msgs += new_msgs
            with consumer.ensure_committed(msgs):
                self._assert_consumer_state(
                    consumer=consumer,
                    actual_msgs=msgs,
                    expected_msg=message,
                    expected_topic=topic,
                    expected_schema_id=registered_schema.schema_id,
                    expected_payload_data=example_payload_data
                )
                assert consumer.message_buffer.empty()

    def _publish_messages(self, producer, message, count):
        for _ in xrange(count):
            producer.publish(message)
        producer.flush()

    def _assert_consumer_state(
            self,
            consumer,
            actual_msgs,
            expected_msg,
            expected_topic,
            expected_schema_id,
            expected_payload_data
    ):
        topic_state = consumer.topic_to_consumer_topic_state_map[expected_topic]
        assert isinstance(topic_state, ConsumerTopicState)
        assert topic_state.last_seen_schema_id == expected_schema_id
        for actual_msg in actual_msgs:
            assert isinstance(actual_msg, Message)
            assert actual_msg.payload == expected_msg.payload
            assert actual_msg.schema_id == expected_msg.schema_id
            assert actual_msg.schema_id == expected_schema_id
            assert actual_msg.topic == expected_msg.topic
            assert actual_msg.topic == expected_topic
            assert actual_msg.payload_data == expected_payload_data
