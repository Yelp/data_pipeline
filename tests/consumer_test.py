# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from tests.consumer_test import BaseConsumerTest
from tests.consumer_test import TIMEOUT


class TestConsumer(BaseConsumerTest):

    @pytest.fixture
    def publish_messages(self, producer, message, consumer):
        def _publish_messages(count):
            assert count > 0
            for _ in xrange(count):
                producer.publish(message)
            producer.flush()
        return _publish_messages

    @pytest.fixture(params=[
        {'force_payload_decode': False},
        {'force_payload_decode': True},
    ])
    def consumer_instance(self, request, topic, team_name):
        return KafkaConsumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None},
            force_payload_decode=request.param['force_payload_decode']
        )

    def test_get_messages_empty(self, consumer, topic,):
        messages = consumer.get_messages(count=10, blocking=True, timeout=TIMEOUT)
        assert len(messages) == 0
        assert consumer.topic_to_consumer_topic_state_map[topic] is None