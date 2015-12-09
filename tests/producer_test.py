# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import binascii
import copy
import math
import multiprocessing
from uuid import UUID

import mock
import pytest
import simplejson as json

import data_pipeline.producer
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from data_pipeline.message import Message
from data_pipeline.message_type import _ProtectedMessageType
from data_pipeline.producer import Producer
from data_pipeline.producer import PublicationUnensurableError
from data_pipeline.testing_helpers.kafka_docker import capture_new_data_pipeline_messages
from data_pipeline.testing_helpers.kafka_docker import capture_new_messages
from data_pipeline.testing_helpers.kafka_docker import setup_capture_new_messages_consumer
from tests.helpers.config import reconfigure


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
    def producer_instance(self, containers, producer_name, use_work_pool, team_name):
        instance = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=use_work_pool
        )
        containers.create_kafka_topic(instance.monitor.monitor_topic)
        return instance

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope='module')
    def topic_two(self, containers):
        topic_two_name = str(binascii.hexlify(FastUUID().uuid4()))
        containers.create_kafka_topic(topic_two_name)
        return topic_two_name

    @pytest.yield_fixture
    def patch_monitor_init_start_time_to_zero(self):
        with mock.patch(
            'data_pipeline.client._Monitor.get_monitor_window_start_timestamp',
            return_value=0
        ) as patched_start_time:
            yield patched_start_time


class TestProducer(TestProducerBase):

    def create_message(self, topic_name, payload, registered_schema, **kwargs):
        return CreateMessage(
            topic=topic_name,
            schema_id=registered_schema.schema_id,
            payload=payload,
            timestamp=1500,
            **kwargs
        )

    @pytest.mark.parametrize("method, skipped_method, kwargs", [
        ('record_message', '_get_record', {'message': None}),
        ('close', 'flush_buffered_info', {}),
    ])
    def test_monitoring_system_disabled(
        self,
        producer_name,
        team_name,
        method,
        skipped_method,
        kwargs
    ):
        producer = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            monitoring_enabled=False
        )
        with mock.patch.object(
            producer.monitor,
            skipped_method
        ) as uncalled_method:
            getattr(producer.monitor, method)(**kwargs)
            assert uncalled_method.called == 0

    def test_basic_publish(self, topic, message, producer, envelope):
        self._publish_and_assert_message(topic, message, producer, envelope)

    def _publish_message(self, topic, message, producer):
        with capture_new_data_pipeline_messages(topic) as get_messages:
            producer.publish(message)
            producer.flush()

            return get_messages()

    def _publish_and_assert_message(self, topic, message, producer, envelope):
        messages = self._publish_message(topic, message, producer)

        assert len(messages) == 1
        assert messages[0].payload == message.payload
        assert messages[0].schema_id == message.schema_id

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
        with reconfigure(encryption_type='AES_MODE_CBC-1'):
            messages = self._publish_message(
                topic,
                self.create_message(
                    topic,
                    payload,
                    registered_schema,
                    contains_pii=True
                ),
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

    def test_publish_encrypted_message_with_pii(
        self,
        topic,
        producer,
        registered_schema
    ):
        payload = b'hello world!'

        with reconfigure(encryption_type='AES_MODE_CBC-1'):
            test_message = self.create_message(
                topic,
                payload,
                registered_schema,
                contains_pii=True
            )
            assert test_message.payload != payload

    def test_publish_encrypted_message_from_payload_data_with_pii(
        self,
        topic,
        producer_instance,
        registered_schema,
    ):
        payload_data = {'good_field': 20}
        with reconfigure(encryption_type='AES_MODE_CBC-1'), reconfigure(skip_messages_with_pii=False):
            with producer_instance as producer:
                test_message = CreateMessage(
                    topic=topic,
                    payload_data=payload_data,
                    schema_id=registered_schema.schema_id,
                    timestamp=1500,
                    contains_pii=True
                )
                messages = self._publish_message(
                    topic,
                    test_message,
                    producer
                )
                assert len(messages) == 1
                assert messages[0].payload_data == test_message.payload_data

    def test_basic_publish_message_with_primary_keys(
        self,
        topic,
        payload,
        producer,
        registered_schema,
        envelope
    ):
        sample_keys = (u'key1=\'', u'key2=\\', u'key3=哎ù\x1f')
        with capture_new_messages(topic) as get_messages:
            producer.publish(
                self.create_message(
                    topic,
                    payload,
                    registered_schema,
                    keys=sample_keys,
                    contains_pii=False
                )
            )
            producer.flush()
            assert get_messages()[0].message.key == '\'key1=\\\'\'\x1f\'key2=\\\\\'\x1f\'key3=哎ù\x1f\''.encode('utf-8')


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
    def topic(self, containers):
        uuid = binascii.hexlify(FastUUID().uuid4())
        topic_name = str("ensure-published-{0}".format(uuid))
        containers.create_kafka_topic(topic_name)
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
            with mock.patch.object(data_pipeline.producer, 'logger') as mock_logger:
                producer.ensure_messages_published(messages, topic_offsets)
            self._assert_all_messages_published(consumer, envelope)
            self._assert_logged_info_correct(mock_logger, 0, topic, topic_offsets)

    def test_ensure_messages_published_when_partially_published(
        self, topic, messages, producer, envelope, topic_offsets
    ):
        with setup_capture_new_messages_consumer(topic) as consumer:
            for message in messages[:2]:
                producer.publish(message)
            producer.flush()
            with mock.patch.object(data_pipeline.producer, 'logger') as mock_logger:
                producer.ensure_messages_published(messages, topic_offsets)
            self._assert_all_messages_published(consumer, envelope)
            self._assert_logged_info_correct(
                mock_logger,
                len(messages[:2]),
                topic,
                topic_offsets
            )

    def test_ensure_messages_published_when_all_published(
        self, topic, messages, producer, envelope, topic_offsets
    ):
        with setup_capture_new_messages_consumer(topic) as consumer:
            for message in messages:
                producer.publish(message)
            producer.flush()
            with mock.patch.object(data_pipeline.producer, 'logger') as mock_logger:
                producer.ensure_messages_published(messages, topic_offsets)
            self._assert_all_messages_published(consumer, envelope)
            self._assert_logged_info_correct(mock_logger, len(messages), topic, topic_offsets)

    def test_ensure_messages_published_fails_when_overpublished(
        self, topic, messages, producer, topic_offsets
    ):
        for message in messages:
            producer.publish(message)
        producer.flush()

        with pytest.raises(PublicationUnensurableError):
            with mock.patch.object(data_pipeline.producer, 'logger') as mock_logger:
                producer.ensure_messages_published(messages[:2], topic_offsets)

        self._assert_logged_info_correct(
            mock_logger,
            len(messages),
            topic,
            topic_offsets,
            message_count=len(messages[:2])
        )

    def test_ensure_messages_published_on_new_topic(self, message, producer):
        """When a topic doesn't exist, all of the messages on that topic should
        be published.
        """
        topic = str(UUID(bytes=FastUUID().uuid4()).hex)
        message.topic = topic
        with mock.patch.object(producer, 'publish') as mock_publish:
            producer.ensure_messages_published([message], {})
            assert mock_publish.call_count == 1

    def _assert_logged_info_correct(
        self,
        mock_logger,
        messages_already_published,
        topic,
        topic_offsets,
        message_count=None
    ):
        if message_count is None:
            message_count = self.number_of_messages
        assert mock_logger.info.call_count == 1
        (log_line,), _ = mock_logger.info.call_args
        logged_info = json.loads(log_line)

        assert logged_info['topic'] == topic
        assert logged_info['message_count'] == message_count
        assert logged_info['saved_offset'] == topic_offsets.get(topic, 0)
        assert logged_info['already_published_count'] == messages_already_published
        assert logged_info['high_watermark'] == (
            topic_offsets.get(topic, 0) + messages_already_published
        )

    def _assert_all_messages_published(self, consumer, envelope):
        messages = consumer.get_messages(count=self.number_of_messages * 2)
        assert len(messages) == self.number_of_messages
        payloads = [
            int(envelope.unpack(message.message.value)['payload'])
            for message in messages
        ]
        assert payloads == range(self.number_of_messages)
