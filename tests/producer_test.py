# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import binascii
import copy
import math
import multiprocessing

import mock
import pytest
from kafka.common import FailedPayloadsError
from kafka.common import ProduceRequest
from kafka.common import ProduceResponse

from data_pipeline._fast_uuid import FastUUID
from data_pipeline._kafka_producer import _EnvelopeAndMessage
from data_pipeline._kafka_producer import _prepare
from data_pipeline._retry_util import ExpBackoffPolicy
from data_pipeline._retry_util import MaxRetryError
from data_pipeline._retry_util import RetryPolicy
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from data_pipeline.message import Message
from data_pipeline.message_type import _ProtectedMessageType
from data_pipeline.producer import Producer
from data_pipeline.producer import PublicationUnensurableError
from tests.helpers.config import reconfigure
from tests.helpers.kafka_docker import capture_new_messages
from tests.helpers.kafka_docker import create_kafka_docker_topic
from tests.helpers.kafka_docker import setup_capture_new_messages_consumer


class RandomException(Exception):
    pass


@pytest.mark.usefixtures(
    "configure_teams",
    "patch_monitor_init_start_time_to_zero"
)
class TestProducerBase(object):

    @pytest.fixture(params=[
        True,
        False
    ])
    def use_work_pool(self, request):
        return request.param

    @pytest.fixture
    def producer_name(self):
        return 'producer_1'

    @pytest.fixture
    def producer_instance(self, producer_name, use_work_pool, team_name):
        return Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=use_work_pool
        )

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope='module')
    def topic(self, kafka_docker, topic_name):
        create_kafka_docker_topic(kafka_docker, topic_name)
        return topic_name

    @pytest.fixture(scope='module')
    def topic_two(self, kafka_docker):
        topic_one_name = str(binascii.hexlify(FastUUID().uuid4()))
        create_kafka_docker_topic(kafka_docker, topic_one_name)
        return topic_one_name

    @pytest.yield_fixture
    def patch_monitor_init_start_time_to_zero(self):
        with mock.patch(
            'data_pipeline.client._Monitor.get_monitor_window_start_timestamp',
            return_value=0
        ) as patched_start_time:
            yield patched_start_time


class TestProducer(TestProducerBase):

    def create_message_with_pii(self, topic_name, payload, registered_schema):
        return CreateMessage(
            topic=topic_name,
            schema_id=registered_schema.schema_id,
            payload=payload,
            timestamp=1500,
            contains_pii=True
        )

    def test_basic_publish(self, topic, message, producer, envelope):
        self._publish_and_assert_message(topic, message, producer, envelope)

    def _publish_message(self, topic, message, producer):
        with capture_new_messages(topic) as get_messages:
            producer.publish(message)
            producer.flush()

            return get_messages()

    def _publish_and_assert_message(self, topic, message, producer, envelope):
        messages = self._publish_message(topic, message, producer)

        assert len(messages) == 1
        unpacked_message = envelope.unpack(messages[0].message.value)
        assert unpacked_message['payload'] == message.payload
        assert unpacked_message['schema_id'] == message.schema_id

    def test_messages_not_duplicated(self, topic, message, producer_instance):
        with capture_new_messages(topic) as get_messages, producer_instance as producer:
            producer.publish(message)
            producer.flush()
        assert len(multiprocessing.active_children()) == 0
        assert len(get_messages()) == 1

    def test_messages_published_without_flush(self, topic, message, producer_instance):
        with capture_new_messages(topic) as get_messages, producer_instance as producer:
            producer.publish(message)
        assert len(multiprocessing.active_children()) == 0
        assert len(get_messages()) == 1

    def test_empty_starting_checkpoint_data(self, producer):
        position_data = producer.get_checkpoint_position_data()
        assert position_data.last_published_message_position_info is None
        assert position_data.topic_to_last_position_info_map == {}
        assert position_data.topic_to_kafka_offset_map == {}

    def test_child_processes_do_not_survive_an_exception(self, producer_instance, message):
        with pytest.raises(RandomException), producer_instance as producer:
            producer.publish(message)
            producer.flush()
            producer.publish(message)
            raise RandomException()
        assert len(multiprocessing.active_children()) == 0

    def test_get_position_data(self, topic, message, producer):
        upstream_info = {'offset': 'fake'}
        message.upstream_position_info = upstream_info
        with setup_capture_new_messages_consumer(topic) as consumer:
            producer.publish(message)
            producer.flush()
            position_data = producer.get_checkpoint_position_data()

            self._verify_position_data(position_data, upstream_info, consumer, producer, message, topic)

    def test_position_data_callback(self, topic, message, producer_name, team_name):
        callback = mock.Mock()
        producer = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            position_data_callback=callback
        )
        upstream_info = {'offset': 'fake'}
        message.upstream_position_info = upstream_info
        with setup_capture_new_messages_consumer(topic) as consumer:
            producer.publish(message)
            producer.flush()
            (position_data,), _ = callback.call_args

            self._verify_position_data(position_data, upstream_info, consumer, producer, message, topic)

    def _verify_position_data(self, position_data, upstream_info, consumer, producer, message, topic):
        # Make sure the position data makes sense
        assert position_data.last_published_message_position_info == upstream_info
        assert position_data.topic_to_last_position_info_map == {topic: upstream_info}
        kafka_offset = position_data.topic_to_kafka_offset_map[topic]

        # The pointer is to the next offset where messages will be
        # published.  There shouldn't be any messages there yet.
        consumer.seek(kafka_offset, 0)  # kafka_offset from head
        assert len(consumer.get_messages(count=10)) == 0

        # publish another message, so we can seek to it
        message.upstream_position_info = {'offset': 'fake2'}
        producer.publish(message)
        producer.flush()

        # There should be a message now that we've published one
        consumer.seek(kafka_offset, 0)  # kafka_offset from head
        assert len(consumer.get_messages(count=10)) == 1

    def test_basic_publish_message_with_pii(
        self,
        topic,
        payload,
        producer,
        registered_schema
    ):
        messages = self._publish_message(
            topic,
            self.create_message_with_pii(topic, payload, registered_schema),
            producer
        )

        assert len(messages) == 0

    def test_basic_publish_message_with_payload_data(
        self,
        topic,
        message_with_payload_data,
        producer,
        envelope
    ):
        self._publish_and_assert_message(
            topic,
            message_with_payload_data,
            producer,
            envelope
        )


class TestPublishMonitorMessage(TestProducerBase):

    def create_message(self, message, topic=None, timestamp=None):
        new_message = copy.deepcopy(message)
        if topic:
            new_message.topic = topic
        if timestamp:
            new_message.timestamp = int(timestamp)
        return new_message

    @property
    def monitor_window_in_sec(self):
        return get_config().monitoring_window_in_sec

    def publish_messages(self, messages, producer):
        for message in messages:
            producer.publish(message)
        producer.flush()
        producer.monitor.flush_buffered_info()

    def get_messages(self, topic, expected_message_count):
        with setup_capture_new_messages_consumer(topic) as consumer:
            consumer.seek(-expected_message_count, 2)
            return consumer.get_messages(count=100)

    def assert_equal_monitor_messages(
        self,
        actual_raw_messages,
        expected_topic,
        expected_messages_counts,
        envelope
    ):
        expected_start_timestamp = 0
        expected_count_idx = 0
        for msg in actual_raw_messages:
            actual_message = self._get_actual_message(msg.message.value, envelope)
            assert actual_message.message_type == _ProtectedMessageType.monitor.name

            payload_data = actual_message.payload_data
            if payload_data['topic'] != expected_topic:
                continue

            self.assert_equal_monitor_message(
                actual_payload_data=payload_data,
                expected_topic=expected_topic,
                expected_message_count=expected_messages_counts[expected_count_idx],
                expected_start_timestamp=expected_start_timestamp
            )
            expected_start_timestamp += self.monitor_window_in_sec
            expected_count_idx += 1

    def _get_actual_message(self, raw_message, envelope):
        unpacked_message = envelope.unpack(raw_message)
        actual_message = Message(
            topic=str('__monitor_topic__'),
            schema_id=unpacked_message['schema_id'],
            payload=unpacked_message['payload'],
            uuid=unpacked_message['uuid'],
            timestamp=unpacked_message['timestamp']
        )
        actual_message._message_type = unpacked_message['message_type']
        return actual_message

    def assert_equal_monitor_message(
        self,
        actual_payload_data,
        expected_topic,
        expected_message_count,
        expected_start_timestamp
    ):
        assert actual_payload_data['message_count'] == expected_message_count
        assert actual_payload_data['client_type'] == 'producer'
        assert actual_payload_data['start_timestamp'] == expected_start_timestamp
        assert actual_payload_data['topic'] == expected_topic

    def test_monitoring_message_basic(self, message, topic, producer, envelope):
        messages_to_publish = [message] * 10
        self.publish_messages(messages_to_publish, producer)

        messages = self.get_messages(topic, len(messages_to_publish))
        assert len(messages) == len(messages_to_publish)

        expected_monitor_message_count = 3
        monitor_messages = self.get_messages(
            producer.monitor.monitor_topic,
            expected_monitor_message_count
        )
        assert len(monitor_messages) == expected_monitor_message_count

        expected_messages_counts = [0] * int(math.floor(
            message.timestamp / self.monitor_window_in_sec
        )) + [len(messages_to_publish)]
        self.assert_equal_monitor_messages(
            actual_raw_messages=monitor_messages,
            expected_topic=topic,
            expected_messages_counts=expected_messages_counts,
            envelope=envelope
        )

    def test_publish_messages_with_diff_timestamps(self, topic, message,
                                                   producer, envelope):
        messages_to_publish = [
            self.create_message(message, timestamp=self.monitor_window_in_sec * 0.5),
            self.create_message(message, timestamp=self.monitor_window_in_sec * 1.5),
            self.create_message(message, timestamp=self.monitor_window_in_sec * 3.5),
        ]
        self.publish_messages(messages_to_publish, producer)

        messages = self.get_messages(topic, len(messages_to_publish))
        assert len(messages) == len(messages_to_publish)

        expected_monitor_message_count = 4
        monitor_messages = self.get_messages(
            producer.monitor.monitor_topic,
            expected_monitor_message_count
        )
        assert len(monitor_messages) == expected_monitor_message_count

        expected_messages_counts = [1, 1, 0, 1]
        self.assert_equal_monitor_messages(
            actual_raw_messages=monitor_messages,
            expected_topic=topic,
            expected_messages_counts=expected_messages_counts,
            envelope=envelope
        )

    def test_publish_messages_with_diff_topic_and_timestamp(
        self,
        topic,
        topic_two,
        message,
        producer,
        envelope
    ):
        message_topic_and_timestamp = [
            (topic, self.monitor_window_in_sec * 0.5),
            (topic_two, self.monitor_window_in_sec * 0.8),
            (topic, self.monitor_window_in_sec * 3.5),
            (topic_two, self.monitor_window_in_sec * 4),
            (topic_two, self.monitor_window_in_sec * 6),
        ]
        messages_to_publish = [
            self.create_message(message, topic=msg_topic, timestamp=msg_timestamp)
            for msg_topic, msg_timestamp in message_topic_and_timestamp
        ]
        self.publish_messages(messages_to_publish, producer)

        assert len(self.get_messages(topic, 2)) == 2
        assert len(self.get_messages(topic_two, 3)) == 3

        expected_monitor_message_count = 14
        monitor_messages = self.get_messages(
            producer.monitor.monitor_topic,
            expected_monitor_message_count
        )
        assert len(monitor_messages) == expected_monitor_message_count

        expected_messages_counts = [1, 0, 0, 1, 0, 0, 0]
        self.assert_equal_monitor_messages(
            actual_raw_messages=monitor_messages,
            expected_topic=topic,
            expected_messages_counts=expected_messages_counts,
            envelope=envelope
        )
        expected_messages_counts = [1, 0, 0, 0, 1, 0, 1]
        self.assert_equal_monitor_messages(
            actual_raw_messages=monitor_messages,
            expected_topic=topic_two,
            expected_messages_counts=expected_messages_counts,
            envelope=envelope
        )

    def test_monitoring_system_dry_run(self, producer_name, team_name):
        producer = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            dry_run=True
        )
        assert producer.monitor.dry_run is True


class TestEnsureMessagesPublished(TestProducerBase):
    number_of_messages = 5

    @pytest.fixture
    def topic(self, kafka_docker):
        uuid = binascii.hexlify(FastUUID().uuid4())
        topic_name = str("ensure-published-{0}".format(uuid))
        create_kafka_docker_topic(kafka_docker, topic_name)
        return topic_name

    @pytest.fixture(params=[True, False])
    def topic_offsets(self, topic, request, producer, message):
        is_fresh_topic = request.param
        if is_fresh_topic:
            return {}
        else:
            message = copy.copy(message)
            message.topic = topic
            message.payload = str("-1")

            producer.publish(message)
            producer.flush()
            return producer.get_checkpoint_position_data().topic_to_kafka_offset_map

    @pytest.fixture
    def messages(self, message, topic):
        messages = [copy.copy(message) for _ in xrange(self.number_of_messages)]
        for i, message in enumerate(messages):
            message.topic = topic
            message.payload = str(i)
        return messages

    def test_ensure_messages_published_without_message(self, topic, producer, topic_offsets):
        with setup_capture_new_messages_consumer(topic) as consumer:
            producer.ensure_messages_published([], topic_offsets)
            assert len(consumer.get_messages(count=self.number_of_messages * 2)) == 0

    def test_ensure_messages_published_when_unpublished(
        self, topic, messages, producer, envelope, topic_offsets
    ):
        with setup_capture_new_messages_consumer(topic) as consumer:
            producer.ensure_messages_published(messages, topic_offsets)
            self._assert_all_messages_published(consumer, envelope)

    def test_ensure_messages_published_when_partially_published(
        self, topic, messages, producer, envelope, topic_offsets
    ):
        with setup_capture_new_messages_consumer(topic) as consumer:
            for message in messages[:2]:
                producer.publish(message)
                producer.flush()
            producer.ensure_messages_published(messages, topic_offsets)
            self._assert_all_messages_published(consumer, envelope)

    def test_ensure_messages_published_when_all_published(
        self, topic, messages, producer, envelope, topic_offsets
    ):
        with setup_capture_new_messages_consumer(topic) as consumer:
            for message in messages:
                producer.publish(message)
                producer.flush()
            producer.ensure_messages_published(messages, topic_offsets)
            self._assert_all_messages_published(consumer, envelope)

    def test_ensure_messages_published_fails_when_overpublished(
        self, topic, messages, producer, topic_offsets
    ):
        for message in messages:
            producer.publish(message)
            producer.flush()

        with pytest.raises(PublicationUnensurableError):
            producer.ensure_messages_published(messages[:2], topic_offsets)

    def _assert_all_messages_published(self, consumer, envelope):
        messages = consumer.get_messages(count=self.number_of_messages * 2)
        assert len(messages) == self.number_of_messages
        payloads = [
            int(envelope.unpack(message.message.value)['payload'])
            for message in messages
        ]
        assert payloads == range(self.number_of_messages)


class TestPublishMessagesWithRetry(TestProducerBase):

    @pytest.fixture
    def another_message(self, topic_two, message):
        new_message = copy.deepcopy(message)
        new_message.topic = topic_two
        return new_message

    @property
    def max_retry_count(self):
        return 3

    @property
    def retry_policy(self):
        return RetryPolicy(
            ExpBackoffPolicy(initial_delay_secs=0.1, max_delay_secs=0.5),
            max_retry_count=self.max_retry_count
        )

    @pytest.fixture(autouse=True)
    def patch_retry_policy(self, producer):
        producer._kafka_producer._publish_retry_policy = self.retry_policy

    def test_publish_all_messages_without_retry(self, topic, message, producer):
        with capture_new_messages(topic) as get_messages:
            producer.publish(message)
            producer.flush()
            messages = get_messages()
        self.assert_equal_msgs(expected_msgs=[message], actual_msgs=messages)

    def test_publish_message_fails_after_retry(self, message, producer):
        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=FailedPayloadsError
        ) as mock_send_request, setup_capture_new_messages_consumer(
            message.topic
        ) as consumer, pytest.raises(
            MaxRetryError
        ):
            producer.publish(message)
            producer.flush()

            messages = consumer.get_messages()
            assert len(messages) == 0
            assert mock_send_request.call_count == self.max_retry_count

    def test_publish_all_messages_after_retry(self, topic, message, producer):
        orig_func = producer._kafka_producer.kafka_client.send_produce_request

        def fail_1st_time_and_succeed_2nd_time(*args, **kwargs):
            def run_original_func(*args, **kwargs):
                return orig_func(*args, **kwargs)
            mock_send_request.side_effect = run_original_func
            return FailedPayloadsError(payload=mock.Mock())

        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=fail_1st_time_and_succeed_2nd_time
        ) as mock_send_request, setup_capture_new_messages_consumer(
            topic
        ) as consumer:
            mock_send_request.reset()
            producer.publish(message)
            producer.flush()

            messages = consumer.get_messages()
            self.assert_equal_msgs(expected_msgs=[message], actual_msgs=messages)
            assert mock_send_request.call_count == 2

    def test_publish_messages_one_succeeds_one_fails_after_retry(
        self,
        message,
        another_message,
        topic,
        topic_two,
        producer
    ):
        mock_response = ProduceResponse(topic, partition=0, error=0, offset=1)
        fail_response = FailedPayloadsError(payload=mock.Mock())
        side_effect = ([[mock_response, fail_response]] +
                       [[fail_response]] * self.max_retry_count)
        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=side_effect
        ), reconfigure(
            kafka_producer_flush_time_limit_seconds=10  # publish all msgs together
        ), pytest.raises(
            MaxRetryError
        ) as e:
            producer.publish(message)
            producer.publish(another_message)
            producer.flush()

        actual_failed_requests, actual_published_msgs_count = e.value.last_result
        assert actual_published_msgs_count == 1
        expected_requests = [ProduceRequest(
            topic=topic_two,
            partition=0,
            messages=[_prepare(_EnvelopeAndMessage(Envelope(), another_message))]
        )]
        assert actual_failed_requests == expected_requests

    def assert_equal_msgs(self, expected_msgs, actual_msgs):
        envelope = Envelope()
        assert len(actual_msgs) == len(expected_msgs)
        for i, actual in enumerate(actual_msgs):
            actual_payload = envelope.unpack(actual.message.value)['payload']
            expected_payload = expected_msgs[i].payload
            assert actual_payload == expected_payload
