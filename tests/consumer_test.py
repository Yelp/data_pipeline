# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random
import time
from multiprocessing import Event
from multiprocessing import Process

import mock
import pytest

from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from tests.base_consumer_test import BaseConsumerTest
from tests.base_consumer_test import MultiTopicsSetupMixin
from tests.base_consumer_test import RefreshDynamicTopicTests
from tests.base_consumer_test import RefreshFixedTopicTests
from tests.base_consumer_test import RefreshNewTopicsTest
from tests.base_consumer_test import SingleSchemaSetupMixin
from tests.base_consumer_test import SingleTopicSetupMixin
from tests.base_consumer_test import TIMEOUT
from tests.base_consumer_test import TopicInDataTargetSetupMixin
from tests.base_consumer_test import TopicInNamespaceSetupMixin
from tests.base_consumer_test import TopicInSourceSetupMixin
from tests.helpers.config import reconfigure


class TestConsumer(BaseConsumerTest):

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
    def pii_topic(self, pii_schema):
        return str(pii_schema.topic.name)

    @pytest.fixture
    def pii_message(self, pii_schema, payload):
        return CreateMessage(
            schema_id=pii_schema.schema_id,
            payload=payload
        )

    @pytest.yield_fixture(autouse=True)
    def setup_encryption_config(self):
        with reconfigure(
            encryption_type='AES_MODE_CBC-1',
            skip_messages_with_pii=False
        ):
            yield

    @pytest.fixture
    def consumer_group_name(self):
        return 'test_consumer_{}'.format(random.random())

    @pytest.fixture
    def consumer_instance(
        self,
        consumer_group_name,
        force_payload_decode,
        topic,
        pii_topic,
        team_name,
        pre_rebalance_callback,
        post_rebalance_callback
    ):
        return Consumer(
            consumer_name=consumer_group_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None, pii_topic: None},
            force_payload_decode=force_payload_decode,
            auto_offset_reset='largest',  # start from the tail of the topic
            pre_rebalance_callback=pre_rebalance_callback,
            post_rebalance_callback=post_rebalance_callback
        )

    @pytest.fixture
    def consumer_two_instance(
        self,
        consumer_group_name,
        force_payload_decode,
        topic,
        pii_topic,
        team_name,
        pre_rebalance_callback,
        post_rebalance_callback
    ):
        return Consumer(
            consumer_name=consumer_group_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None, pii_topic: None},
            force_payload_decode=force_payload_decode,
            pre_rebalance_callback=pre_rebalance_callback,
            post_rebalance_callback=post_rebalance_callback
        )

    def test_offset_cache_cleared_after_rebalance(
        self,
        topic,
        pii_topic,
        publish_messages,
        consumer_instance,
        consumer_two_instance,
        message,
        pii_message
    ):
        """
        This test starts a consumer (consumer_one) with two topics and
        retrieves a message and starts another consumer (consumer_two)
        with the same name in a separate process. This test asserts that
        everytime a consumer under goes rebalance
        topic_to_partition_offset_map_cache is reset.
        """
        consumer_one_rebalanced_event = Event()
        with consumer_instance as consumer_one:
            # publishing messages on two topics
            publish_messages(message, count=10)
            publish_messages(pii_message, count=10)

            consumer_two_process = Process(
                target=self._run_consumer_two,
                args=(consumer_two_instance, consumer_one_rebalanced_event)
            )
            consumer_two_process.start()
            for _ in range(2):
                consumer_one.commit_message(
                    consumer_one.get_message(blocking=True, timeout=TIMEOUT)
                )
            assert len(consumer_one.topic_to_partition_offset_map_cache) > 0

            consumer_one_rebalanced_event.set()
            consumer_two_process.join()

            consumer_one_msgs = []
            # post rebalance callback is not called untill consumer
            # talks to kafka
            for _ in range(8):
                consumer_one_msgs.append(consumer_one.get_message(
                    blocking=True, timeout=TIMEOUT
                ))
            assert len(consumer_one.topic_to_partition_offset_map_cache) == 0

            consumer_one.commit_messages(consumer_one_msgs)
            assert len(consumer_one.topic_to_partition_offset_map_cache) > 0

    def test_sync_topic_consumer_map(
        self,
        topic,
        pii_topic,
        publish_messages,
        consumer_instance,
        consumer_two_instance,
        message,
        pii_message
    ):
        """
        This test starts a consumer (consumer_one) with two topics and
        retrieves a message and starts another consumer (consumer_two)
        with the same name in a separate process and while the
        consumer_two is retrieving messages asserts that the topic
        redistribution occurs and the topic_to_consumer_topic_state_map
        for consumer_two is with only one of the two topics. Then it
        stops consumer_two process first and again asserts that all
        the original topics have been reassigned to consumer one.
        """
        consumer_one_rebalanced_event = Event()
        with consumer_instance as consumer_one:
            # publishing messages on two topics
            publish_messages(message, count=10)
            publish_messages(pii_message, count=10)

            consumer_two_process = Process(
                target=self._run_consumer_two,
                args=(consumer_two_instance, consumer_one_rebalanced_event)
            )
            consumer_two_process.start()

            # Consumer one needs to continue to receive messages while consumer two
            # starts so that when consumer two starts the topics are distributed.
            for _ in range(2):
                consumer_one.get_message(blocking=True, timeout=TIMEOUT)
                # TODO: https://jira.yelpcorp.com/browse/DATAPIPE-752
                time.sleep(1)
            assert len(consumer_one.topic_to_consumer_topic_state_map) == 1

            consumer_one_rebalanced_event.set()
            consumer_two_process.join()

            for _ in range(8):
                consumer_one.get_message(blocking=True, timeout=TIMEOUT)
            assert len(consumer_one.topic_to_consumer_topic_state_map) == 2

    def _run_consumer_two(self, consumer_instance, rebalanced_event):
        with consumer_instance as another_consumer:
            assert len(another_consumer.topic_to_consumer_topic_state_map) == 1
            another_consumer.get_message(blocking=True, timeout=TIMEOUT)
            rebalanced_event.wait()

            for _ in range(9):
                another_consumer.get_message(blocking=True, timeout=TIMEOUT)


class TestRefreshTopics(RefreshNewTopicsTest):

    @pytest.fixture
    def consumer_instance(self, topic, team_name):
        return Consumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None}
        )


class ConsumerRefreshFixedTopicTests(RefreshFixedTopicTests):

    @pytest.fixture
    def consumer_instance(self, topic, team_name):
        return Consumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None}
        )


class TestRefreshSingleTopic(
    ConsumerRefreshFixedTopicTests,
    SingleTopicSetupMixin
):
    pass


class TestRefreshMultiTopics(
    ConsumerRefreshFixedTopicTests,
    MultiTopicsSetupMixin
):
    pass


class TestRefreshSingleSchema(
    ConsumerRefreshFixedTopicTests,
    SingleSchemaSetupMixin
):
    pass


class ConsumerRefreshDynamicTopicTests(RefreshDynamicTopicTests):

    @pytest.fixture
    def consumer_instance(self, topic, team_name):
        return Consumer(
            consumer_name='test_consumer',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map={topic: None}
        )


class TestRefreshTopicInNamespace(
    ConsumerRefreshDynamicTopicTests,
    TopicInNamespaceSetupMixin
):
    pass


class TestRefreshTopicInSource(
    ConsumerRefreshDynamicTopicTests,
    TopicInSourceSetupMixin
):
    pass


class TestRefreshTopicInDataTarget(
    ConsumerRefreshDynamicTopicTests,
    TopicInDataTargetSetupMixin
):
    pass
