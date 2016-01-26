# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Event
from multiprocessing import Process

import mock
import pytest

from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from tests.base_consumer_test import BaseConsumerTest
from tests.base_consumer_test import RefreshTopicsTest
from tests.base_consumer_test import TIMEOUT


class TestConsumer(BaseConsumerTest):

    @pytest.fixture
    def publish_messages(self, producer, message, consumer):
        def _publish_messages(count):
            assert count > 0
            for _ in xrange(count):
                producer.publish(message)
            producer.flush()
        return _publish_messages

    @pytest.fixture
    def pre_rebalance_callback(self):
        return mock.Mock()

    @pytest.fixture
    def post_rebalance_callback(self):
        return mock.Mock()

    @pytest.fixture(params=[False, True])
    def force_payload_decode(self, request):
        return request.param

    @pytest.fixture
    def consumer_instance(
            self,
            force_payload_decode,
            topic,
            topic_two,
            team_name,
            pre_rebalance_callback,
            post_rebalance_callback
    ):
        return Consumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None, topic_two: None},
            force_payload_decode=force_payload_decode,
            pre_rebalance_callback=pre_rebalance_callback,
            post_rebalance_callback=post_rebalance_callback
        )

    def _publish_message(self, topic, message, producer, n_times=1):
        for _ in range(n_times):
            producer.publish(message)
            producer.flush()

    def test_get_messages_empty(self, consumer, topic):
        messages = consumer.get_messages(
            count=10,
            blocking=True,
            timeout=TIMEOUT
        )
        assert len(messages) == 0
        assert consumer.topic_to_consumer_topic_state_map[topic] is None

    def test_sync_topic_consumer_map(
            self,
            force_payload_decode,
            topic,
            topic_two,
            team_name,
            pre_rebalance_callback,
            post_rebalance_callback,
            publish_messages,
            producer,
            consumer,
            message
    ):
        """
        This test starts a consumer with two topics and retrieves a message
        and starts another consumer with the same name in a separate thread
        and while the second consumer is retrieving messages
        asserts that the topic redistribution occurs and the
        topic_to_consumer_topic_state_map for the second consumer is
        with only one of the two topics
        """

        event = Event()
        event_two = Event()

        # publishing messages on two topics
        self._publish_message(topic, message, producer, 10)

        message.topic = topic_two
        self._publish_message(topic_two, message, producer, 10)

        second_consumer_process = Process(
            target=self._second_consumer,
            args=(
                team_name,
                topic,
                topic_two,
                ExpectedFrequency.constantly,
                force_payload_decode,
                pre_rebalance_callback,
                post_rebalance_callback,
                event,
                event_two
            )
        )
        second_consumer_process.start()

        # Consumer one needs to continue to receive messages while consumer two
        # starts so that when consumer two starts the topics are distributed.
        for _ in range(2):
            consumer.get_message(blocking=True, timeout=TIMEOUT)

        event_two.wait()

        assert len(consumer.topic_to_consumer_topic_state_map) == 1

        event.set()

        second_consumer_process.join()

        for _ in range(6):
            consumer.get_message(blocking=True, timeout=TIMEOUT)

        assert len(consumer.topic_to_consumer_topic_state_map) == 2

    def _second_consumer(
        self,
        team_name,
        topic,
        topic_two,
        expected_frequency_seconds,
        force_payload_decode,
        pre_rebalance_callback,
        post_rebalance_callback,
        event,
        event_two
    ):
        """
        The consumer names should be the same for the partitioner to
        redistribute the topics among them
        """
        with Consumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None, topic_two: None},
            force_payload_decode=force_payload_decode,
            pre_rebalance_callback=pre_rebalance_callback,
            post_rebalance_callback=post_rebalance_callback
        ) as consumer_two:
            assert len(consumer_two.topic_to_consumer_topic_state_map) == 1
            consumer_two.get_message(blocking=True, timeout=TIMEOUT)
            event_two.set()
            event.wait()

            for _ in range(8):
                consumer_two.get_message(blocking=True, timeout=TIMEOUT)


class TestRefreshTopics(RefreshTopicsTest):

    @pytest.fixture
    def consumer_instance(self, topic, team_name, pre_rebalance_callback, post_rebalance_callback):
        return Consumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None}
        )
