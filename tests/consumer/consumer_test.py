# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import random
import time
from multiprocessing import Event
from multiprocessing import Process
from uuid import uuid4

import clog
import mock
import pytest
from kafka.common import FailedPayloadsError

from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.consumer import Consumer
from data_pipeline.consumer_source import FixedSchemas
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from tests.consumer.base_consumer_test import BaseConsumerSourceBaseTest
from tests.consumer.base_consumer_test import BaseConsumerTest
from tests.consumer.base_consumer_test import FixedSchemasSetupMixin
from tests.consumer.base_consumer_test import MultiTopicsSetupMixin
from tests.consumer.base_consumer_test import RefreshDynamicTopicTests
from tests.consumer.base_consumer_test import RefreshFixedTopicTests
from tests.consumer.base_consumer_test import RefreshNewTopicsTest
from tests.consumer.base_consumer_test import SingleTopicSetupMixin
from tests.consumer.base_consumer_test import TIMEOUT
from tests.consumer.base_consumer_test import TopicInDataTargetSetupMixin
from tests.consumer.base_consumer_test import TopicInSourceAutoRefreshSetupMixin
from tests.consumer.base_consumer_test import TopicInSourceSetupMixin
from tests.consumer.base_consumer_test import TopicsInFixedNamespacesAutoRefreshSetupMixin
from tests.consumer.base_consumer_test import TopicsInFixedNamespacesSetupMixin
from tests.helpers.config import reconfigure
from tests.helpers.mock_utils import attach_spy_on_func


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

    def test_offset_retry_on_network_flake(
        self,
        consumer_instance
    ):
        mock_offsets = {
            'test-topic': {0: 10}
        }
        exception = FailedPayloadsError("Network flake!")
        with mock.patch.object(
            consumer_instance.kafka_client,
            'send_offset_commit_request',
            side_effect=[
                exception,
                exception,
                exception,
                None
            ]
        ) as mock_send_offsets, mock.patch.object(
            consumer_instance,
            '_get_offsets_map_to_be_committed',
            return_value=mock_offsets
        ):
            consumer_instance.commit_offsets(mock_offsets)
            assert mock_send_offsets.call_count == 4

    # TODO This is a flakey test that needs to be fixed
    # DATAPIPE-1307
    def skip_test_offset_cache_cleared_at_rebalance(
        self,
        topic,
        pii_topic,
        publish_messages,
        consumer_instance,
        consumer_two_instance,
        message,
        pii_message
    ):
        # TODO [DATAPIPE-249] previous version of test has an issue that
        # sometimes the consumer one doesn't get any message right after
        # consumer two starts.  It's unclear the cause and may be related
        # to how the tests are setup. Re-writting the test to bypass it
        # and defer addressing it in the DATAPIPE-249.
        consumer_one_rebalanced_event = Event()
        with consumer_instance as consumer_one:
            publish_messages(message, count=10)
            publish_messages(pii_message, count=10)

            consumer_one_message = consumer_one.get_message(
                blocking=True,
                timeout=TIMEOUT
            )
            consumer_one.commit_message(consumer_one_message)

            # trigger rebalancing by starting another consumer with same name
            consumer_two_process = Process(
                target=self._run_consumer_two,
                args=(consumer_two_instance, consumer_one_rebalanced_event)
            )
            consumer_two_process.start()
            # consumer one is rebalanced during `get_message`
            consumer_one.get_message(blocking=True, timeout=TIMEOUT)
            consumer_one_rebalanced_event.set()

            consumer_two_process.join(timeout=1)
            assert not consumer_two_process.exitcode

            # force consumer rebalance again; consumer rebalance occurs when
            # get_message is called; set short timeout because we don't care
            # if there is any message left.
            consumer_one.get_message(blocking=True, timeout=0.1)

            # The same offset should be committed again because the rebalancing
            # will clear the internal offset cache.
            with attach_spy_on_func(
                consumer_one.kafka_client,
                'send_offset_commit_request'
            ) as func_spy:
                consumer_one.commit_message(consumer_one_message)
                assert func_spy.call_count == 1

    def test_sync_topic_partition_map(
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
        redistribution occurs and the topic_to_parition_map
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
            assert len(consumer_one.topic_to_partition_map) == 1

            consumer_one_rebalanced_event.set()
            consumer_two_process.join()

            consumer_one.get_message(blocking=True, timeout=TIMEOUT)
            assert len(consumer_one.topic_to_partition_map) == 2

    def _run_consumer_two(self, consumer_instance, rebalanced_event):
        with consumer_instance as another_consumer:
            assert len(another_consumer.topic_to_partition_map) == 1
            another_consumer.get_message(blocking=True, timeout=TIMEOUT)
            rebalanced_event.wait()

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


class ConsumerAutoRefreshTest(BaseConsumerSourceBaseTest):

    @pytest.fixture
    def refresh_namespace(self):
        return "auto_refresh_namespace_{}".format(uuid4())

    @pytest.fixture
    def refresh_source(self):
        return "auto_refresh_source_{}".format(uuid4())

    @pytest.fixture
    def registered_auto_refresh_schema(
        self,
        schematizer_client,
        example_schema,
        refresh_namespace,
        refresh_source
    ):
        return schematizer_client.register_schema(
            namespace=refresh_namespace,
            source=refresh_source,
            schema_str=example_schema,
            source_owner_email='test@yelp.com',
            contains_pii=False
        )

    @pytest.fixture
    def current_auto_topic(self, registered_auto_refresh_schema):
        return str(registered_auto_refresh_schema.topic.name)

    @pytest.fixture
    def current_message(self, registered_auto_refresh_schema, payload):
        return CreateMessage(
            schema_id=registered_auto_refresh_schema.schema_id,
            payload=payload
        )

    def _setup_new_topic_and_publish_message_helper(
        self,
        schematizer_client,
        publish_messages,
        schema,
        payload_data,
        namespace,
        source,
        message_count
    ):
        registered_non_compatible_schema = schematizer_client.register_schema(
            namespace=namespace,
            source=source,
            schema_str=schema,
            source_owner_email='test@yelp.com',
            contains_pii=False
        )
        message = CreateMessage(
            schema_id=registered_non_compatible_schema.schema_id,
            payload_data=payload_data
        )
        publish_messages(message, count=message_count)
        return message

    @pytest.fixture
    def non_compatible_payload_data(self, example_non_compatible_schema):
        return {
            "good_field": 11,
            "good_non_compatible_field": "non_compatible_value"
        }

    @pytest.fixture
    def consumer_instance(
        self,
        consumer_group_name,
        team_name,
        consumer_source,
        pre_rebalance_callback,
        post_rebalance_callback
    ):
        return Consumer(
            consumer_name=consumer_group_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            consumer_source=consumer_source,
            topic_refresh_frequency_seconds=0.5,
            auto_offset_reset='smallest',
            pre_rebalance_callback=pre_rebalance_callback,
            post_rebalance_callback=post_rebalance_callback,
        )

    def test_consumer_pick_up_new_topics_after_refresh(
        self,
        schematizer_client,
        publish_messages,
        consumer_instance,
        example_non_compatible_schema,
        non_compatible_payload_data,
        current_message,
        refresh_namespace,
        refresh_source
    ):
        """
        This test publishes 2 messages and starts the consumer. The consumer
        should receive exactly those 2 messages. It then commits those messages
        and publishes 3 new messages in a different topic. Verifies that the
        consumer refreshes itself to include the new topic and receives exactly
        those 3 messages.
        """
        publish_messages(current_message, count=2)
        with consumer_instance as consumer:
            # consumer should return exactly 2 messages
            actual_messages = consumer.get_messages(
                count=10, blocking=True, timeout=TIMEOUT
            )
            self.assert_equal_messages(actual_messages, current_message, 2)
            assert len(consumer.topic_to_partition_map) == 1

            consumer.commit_messages(actual_messages)

            next_auto_message = self._setup_new_topic_and_publish_message_helper(
                schematizer_client,
                publish_messages,
                schema=example_non_compatible_schema,
                payload_data=non_compatible_payload_data,
                namespace=refresh_namespace,
                source=refresh_source,
                message_count=1
            )

            # consumer should refresh itself and include the new topic in the
            # topic_to_partition_map
            new_messages = consumer.get_messages(
                count=10, blocking=True, timeout=TIMEOUT
            )
            self.assert_equal_messages(new_messages, next_auto_message, 1)
            assert len(consumer.topic_to_partition_map) == 2

    def assert_equal_messages(
        self,
        actual_messages,
        expected_message,
        expected_count
    ):
        assert isinstance(actual_messages, list)
        assert len(actual_messages) == expected_count
        for actual_message in actual_messages:
            assert actual_message.message_type == expected_message.message_type
            assert actual_message.payload == expected_message.payload
            assert actual_message.schema_id == expected_message.schema_id
            assert actual_message.topic == expected_message.topic
            assert actual_message.payload_data == expected_message.payload_data


class TestReaderSchemaMapFixedSchemas(BaseConsumerSourceBaseTest):

    @pytest.fixture(scope='class')
    def topic(self, registered_schema, containers):
        topic_name = str(registered_schema.topic.name)
        containers.create_kafka_topic(topic_name)
        return topic_name

    @pytest.fixture
    def consumer_instance(
        self,
        consumer_group_name,
        force_payload_decode,
        topic,
        team_name,
        pre_rebalance_callback,
        post_rebalance_callback,
        registered_schema,
        registered_compatible_schema,
        registered_non_compatible_schema
    ):
        consumer_source = FixedSchemas(
            registered_schema.schema_id,
            registered_non_compatible_schema.schema_id
        )

        return Consumer(
            consumer_name=consumer_group_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            topic_to_consumer_topic_state_map=None,
            consumer_source=consumer_source,
            force_payload_decode=force_payload_decode,
            auto_offset_reset='largest',  # start from the tail of the topic
            pre_rebalance_callback=pre_rebalance_callback,
            post_rebalance_callback=post_rebalance_callback,
        )

    @pytest.fixture
    def registered_non_compatible_schema(
        self,
        schematizer_client,
        example_non_compatible_schema,
        namespace,
        source
    ):
        return schematizer_client.register_schema(
            namespace=namespace,
            source=source,
            schema_str=example_non_compatible_schema,
            source_owner_email='test@yelp.com',
            contains_pii=False
        )

    @pytest.fixture
    def expected_message(
        self,
        registered_schema,
        payload
    ):
        return CreateMessage(
            schema_id=registered_schema.schema_id,
            payload=payload
        )

    @pytest.fixture
    def input_compatible_message(
        self,
        registered_compatible_schema,
        compatible_payload_data
    ):
        return CreateMessage(
            schema_id=registered_compatible_schema.schema_id,
            payload_data=compatible_payload_data
        )

    def test_get_messages_uses_correct_reader_schema(
        self,
        topic,
        publish_messages,
        consumer_instance,
        input_compatible_message,
        expected_message,
        containers
    ):
        """
        This test publishes a message and initializes a consumer with different
        consumer sources. Then it starts the consumer and retrieves the message
        while passing reader_schema_id. Verifies that if reader_schema_id is
        provided then message gets decoded using that reader_schema_id else
        message gets decoded using the schema_id the message was encoded with.
        """
        with consumer_instance as consumer:
            publish_messages(input_compatible_message, count=1)
            actual_message = consumer.get_message(blocking=True, timeout=TIMEOUT)
            assert actual_message.reader_schema_id == expected_message.schema_id
            assert actual_message.payload_data == expected_message.payload_data


class TestConsumerRegistration(TestReaderSchemaMapFixedSchemas):

    def test_consumer_initial_registration_message(self, topic):
        """
        Assert that an initial RegistrationMessage is sent upon starting
        the Consumer with a non-empty topic_to_consumer_topic_state_map.
        """
        with attach_spy_on_func(
            clog,
            'log_line'
        ) as func_spy:
            fake_topic = ConsumerTopicState({}, 23)
            with Consumer(
                consumer_name='test_consumer',
                team_name='bam',
                expected_frequency_seconds=ExpectedFrequency.constantly,
                topic_to_consumer_topic_state_map={topic: fake_topic}
            ):
                assert func_spy.call_count == 1

    def test_consumer_periodic_registration_messages(
        self,
        publish_messages,
        input_compatible_message,
        consumer_instance
    ):
        """
        This function tests whether a Consumer correctly periodically creates and
        sends registration messages once it has received messages from a topic it
        is consuming from.

        Note: Tests fails when threshold is set significanly below 1 second
        """
        TIMEOUT = 1.8
        consumer_instance.registrar.threshold = 1
        with consumer_instance as consumer:
            with attach_spy_on_func(
                consumer.registrar.clog_writer,
                'publish'
            ) as func_spy:
                publish_messages(input_compatible_message, count=1)
                consumer.get_message(blocking=True, timeout=TIMEOUT)
                consumer.registrar.threshold = 1
                consumer.registrar.start()
                time.sleep(2.5)
                assert func_spy.call_count == 2
                consumer.registrar.stop()

    def test_consumer_registration_message_on_exit(
        self,
        publish_messages,
        input_compatible_message,
        consumer_instance
    ):
        TIMEOUT = 1.8
        consumer = consumer_instance.__enter__()
        with attach_spy_on_func(
            consumer.registrar,
            'stop'
        ) as func_spy:
            publish_messages(input_compatible_message, count=1)
            consumer.get_message(blocking=True, timeout=TIMEOUT)
            consumer.__exit__(None, None, None)
            assert func_spy.call_count == 1


class TestAutoRefreshConsumerTopicsInFixedNamespaces(
    ConsumerAutoRefreshTest,
    TopicsInFixedNamespacesAutoRefreshSetupMixin
):
    pass


class TestAutoRefreshConsumerTopicInSource(
    ConsumerAutoRefreshTest,
    TopicInSourceAutoRefreshSetupMixin
):
    pass


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


class TestRefreshFixedSchemas(
    ConsumerRefreshFixedTopicTests,
    FixedSchemasSetupMixin
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


class TestRefreshTopicsInFixedNamespaces(
    ConsumerRefreshDynamicTopicTests,
    TopicsInFixedNamespacesSetupMixin
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
