# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import ast
import binascii
import copy
import multiprocessing

import mock
import pytest

from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from data_pipeline.message import Message
from data_pipeline.producer import Producer
from data_pipeline.producer import PublicationUnensurableError
from data_pipeline.test_helpers.kafka_docker import capture_new_data_pipeline_messages
from data_pipeline.test_helpers.kafka_docker import capture_new_messages
from data_pipeline.test_helpers.kafka_docker import create_kafka_docker_topic
from data_pipeline.test_helpers.kafka_docker import setup_capture_new_messages_consumer


class RandomException(Exception):
    pass


@pytest.mark.usefixtures("patch_dry_run", "configure_teams")
class TestProducerBase(object):

    @pytest.yield_fixture
    def patch_dry_run(self):
        with mock.patch.object(
            Message,
            'dry_run',
            new_callable=mock.PropertyMock
        ) as mock_dry_run:
            mock_dry_run.return_value = True
            yield mock_dry_run

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
    def topic_1(self, kafka_docker):
        create_kafka_docker_topic(kafka_docker, str('topic-1'))
        return str('topic-1')

    @pytest.fixture(scope='module')
    def monitor(self, kafka_docker):
        create_kafka_docker_topic(kafka_docker, str('message-monitoring-log'))
        return str('message-monitoring-log')


class TestProducer(TestProducerBase):

    def create_message_with_pii(self, topic_name, payload, registered_schema):
        return CreateMessage(
            topic=topic_name,
            schema_id=registered_schema.schema_id,
            payload=payload,
            timestamp=1500,
            contains_pii=True
        )

    def create_message_with_specified_timestamp(self, topic_name, payload, timestamp):
        """returns a message with a specified timestamp
        """
        return CreateMessage(
            topic=topic_name,
            schema_id=10,
            payload=payload,
            timestamp=timestamp
        )

    def assert_monitoring_system_checks(self, unpacked_message, topic, message_count, message_timeslot):
        assert unpacked_message['message_type'] == 'monitor'
        decoded_payload = ast.literal_eval(unpacked_message['payload'])
        assert decoded_payload['message_count'] == message_count
        assert decoded_payload['client_type'] == 'producer'
        assert decoded_payload['start_timestamp'] == message_timeslot * get_config().monitoring_window_in_sec
        assert decoded_payload['topic'] == topic

    def test_monitoring_message_basic(self, message, topic, monitor, producer, envelope):
        with capture_new_messages(topic) as get_messages, \
                capture_new_messages(monitor) as get_monitoring_messages:
            for i in xrange(99):
                producer.publish(message)
            producer.flush()
            producer.monitoring_message.flush_buffered_info()
            messages = get_messages()
            monitoring_messages = get_monitoring_messages()

        assert len(messages) == 99
        assert len(monitoring_messages) == 3

        # first and second monitoring messages will have 0 as the message_count
        # since the timestamp of published message is 1500
        unpacked_message = envelope.unpack(monitoring_messages[0].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=0,
            message_timeslot=0
        )

        unpacked_message = envelope.unpack(monitoring_messages[1].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=0,
            message_timeslot=1
        )

        # third monitoring_message will again have 99 as the message_count
        unpacked_message = envelope.unpack(monitoring_messages[2].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=99,
            message_timeslot=2
        )

    def test_monitoring_system_same_topic_different_timestamp_messages(
        self,
        topic,
        monitor,
        payload,
        producer,
        envelope
    ):
        # list of timestamps for which message will be created and published for
        # testing purposes
        timestamp_list = [100, 654, 2010, 2015, 2050]
        with capture_new_messages(topic) as get_messages, \
                capture_new_messages(monitor) as get_monitoring_messages:
            for timestamp in timestamp_list:
                producer.publish(self.create_message_with_specified_timestamp(topic, payload, timestamp))
            producer.flush()
            producer.monitoring_message.flush_buffered_info()
            monitoring_messages = get_monitoring_messages()
            messages = get_messages()

        assert len(messages) == 5
        assert len(monitoring_messages) == 4

        # first and second monitoring messages should have count as 1
        unpacked_message = envelope.unpack(monitoring_messages[0].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=1,
            message_timeslot=0
        )

        unpacked_message = envelope.unpack(monitoring_messages[1].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=1,
            message_timeslot=1
        )

        # third monitoring message should have message_count as 0
        # since no messages are published with the timestamp between 1200-1800
        unpacked_message = envelope.unpack(monitoring_messages[2].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=0,
            message_timeslot=2
        )

        # forth monitoring message should have message count as 3
        unpacked_message = envelope.unpack(monitoring_messages[3].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=3,
            message_timeslot=3
        )

    def test_monitoring_system_different_topic_different_timestamp_messages(
        self,
        topic,
        topic_1,
        monitor,
        payload,
        producer,
        envelope,
        kafka_docker
    ):
        timestamp_topic_name_list = [
            (1020, topic),
            (1023, topic_1),
            (1043, topic),
            (1034, topic_1),
            (1079, topic_1),
            (2025, topic_1),
        ]
        with capture_new_messages(monitor) as get_monitoring_messages, \
                capture_new_messages(topic) as get_messages, \
                capture_new_messages(topic_1) as get_messages_for_topic_1:
            for timestamp, topic_name in timestamp_topic_name_list:
                producer.publish(
                    self.create_message_with_specified_timestamp(
                        topic_name,
                        payload,
                        timestamp
                    )
                )
            producer.flush()
            producer.monitoring_message.flush_buffered_info()
            topic_1_messages = get_messages_for_topic_1()
            topic_messages = get_messages()
            monitoring_messages = get_monitoring_messages()

        # verifying messages are published properly
        assert len(topic_messages) == 2
        assert len(topic_1_messages) == 4

        # varifying number of monitoring_messages
        assert len(monitoring_messages) == 6

        # first and second monitoring messages will have 0 message_count
        # and would be for topic and topic-1 respectively
        unpacked_message = envelope.unpack(monitoring_messages[0].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic,
            message_count=0,
            message_timeslot=0
        )

        unpacked_message = envelope.unpack(monitoring_messages[1].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic_1,
            message_count=0,
            message_timeslot=0
        )

        # third and forth monitoring messages should be for topic-1 and have 3 and 0
        # as the message_count since 3 messages of topic 'topic-1' were published
        # in timeslot 600 - 1200 but none for timeslot 1200-1800
        unpacked_message = envelope.unpack(monitoring_messages[2].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic_1,
            message_count=3,
            message_timeslot=1
        )

        unpacked_message = envelope.unpack(monitoring_messages[3].message.value)
        self.assert_monitoring_system_checks(
            unpacked_message,
            topic_1,
            message_count=0,
            message_timeslot=2
        )

        # fifth and sixth message would be published as a part of flushing
        # monitoring_message and should be for topic-1 and topic. However, since
        # flushing process traverses through the topic_tracking_info_map, one
        # cannot guarantee which tracking_info will be published first
        unpacked_message = envelope.unpack(monitoring_messages[4].message.value)
        decoded_payload = ast.literal_eval(unpacked_message['payload'])
        if decoded_payload['topic'] == topic:
            self.assert_monitoring_system_checks(
                unpacked_message,
                topic,
                message_count=2,
                message_timeslot=1
            )

            unpacked_message = envelope.unpack(monitoring_messages[5].message.value)
            self.assert_monitoring_system_checks(
                unpacked_message,
                topic_1,
                message_count=1,
                message_timeslot=3
            )
        else:
            self.assert_monitoring_system_checks(
                unpacked_message,
                topic_1,
                message_count=1,
                message_timeslot=3
            )
            unpacked_message = envelope.unpack(monitoring_messages[5].message.value)
            self.assert_monitoring_system_checks(
                unpacked_message,
                topic,
                message_count=2,
                message_timeslot=1
            )

    def test_monitoring_system_dry_run(self, producer_name, team_name):
        producer = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            dry_run=True
        )
        assert producer.monitoring_message.dry_run is True

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
