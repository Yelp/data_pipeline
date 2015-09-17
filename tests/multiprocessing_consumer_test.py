# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import pytest

from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.multiprocessing_consumer import MultiprocessingConsumer
from tests.base_consumer_test import BaseConsumerTest
from tests.base_consumer_test import TIMEOUT


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
        {'force_payload_decode': False},
        {'force_payload_decode': True},
    ])
    def consumer_instance(self, request, topic, kafka_docker, team_name):
        return MultiprocessingConsumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None},
            max_buffer_size=self.test_buffer_size,
            force_payload_decode=request.param['force_payload_decode']
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
