# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import ast
import multiprocessing

import mock
import pytest

from data_pipeline import lazy_message
from data_pipeline.async_producer import AsyncProducer
from data_pipeline.config import get_config
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from data_pipeline.producer import Producer
from tests.helpers.kafka_docker import capture_new_messages
from tests.helpers.kafka_docker import create_kafka_docker_topic
from tests.helpers.kafka_docker import setup_capture_new_messages_consumer


class RandomException(Exception):
    pass


@pytest.mark.usefixtures("patch_dry_run")
class TestProducer(object):

    @pytest.yield_fixture
    def patch_dry_run(self):
        with mock.patch.object(
            lazy_message.LazyMessage,
            'dry_run',
            new_callable=mock.PropertyMock
        ) as mock_dry_run:
            mock_dry_run.return_value = True
            yield mock_dry_run

    @pytest.fixture(params=[
        Producer,
        AsyncProducer
    ])
    def producer_klass(self, request):
        return request.param

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
    def producer_instance(self, producer_klass, producer_name, use_work_pool):
        return producer_klass(producer_name=producer_name, use_work_pool=use_work_pool)

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope='module')
    def topic(self, kafka_docker, topic_name):
        create_kafka_docker_topic(kafka_docker, topic_name)
        create_kafka_docker_topic(kafka_docker, str('message-monitoring-log'))
        return topic_name

    @pytest.fixture(scope='module')
    def topic_1(self, kafka_docker):
        create_kafka_docker_topic(kafka_docker, str('topic-1'))
        return str('topic-1')

    @pytest.fixture
    def lazy_message(self, topic_name):
        return lazy_message.LazyMessage(topic_name, 10, {1: 100}, MessageType.create, timestamp=1456)

    def create_message_with_specified_timestamp(self, topic_name, payload, timestamp):
        """returns a message with a specified timestamp
        """
        return Message(
            topic_name,
            10,
            payload,
            MessageType.create,
            timestamp=timestamp
        )

    def assert_monitoring_system_checks(self, unpacked_message, message_count, message_timeslot, topic):
        assert unpacked_message['message_type'] == 'monitor'
        decoded_payload = ast.literal_eval(unpacked_message['payload'])
        assert decoded_payload['message_count'] == message_count
        assert decoded_payload['client_type'] == 'producer'
        assert decoded_payload['start_timestamp'] == message_timeslot * get_config().monitoring_window_in_sec
        assert decoded_payload['topic'] == topic

    def test_monitoring_message_basic(self, message, topic, producer, envelope):
        with capture_new_messages(topic) as get_messages, \
                capture_new_messages('message-monitoring-log') as get_monitoring_messages:
            for i in xrange(99):
                producer.publish(message)
            producer.flush()
            producer.monitoring_message.flush_buffered_info()
            messages = get_messages()
            monitoring_messages = get_monitoring_messages()

        assert len(messages) == 99
        assert len(monitoring_messages) == 3

        # The first message will have 0 as the message_count
        # since the timestamp of message is above 600 (outside the monitored window)
        unpacked_message = envelope.unpack(monitoring_messages[0].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 0, 0, topic)

        # second monitoring_message will again have 0 as the message_count
        # since the message timestamp is outside the 600-1200 timeslot
        unpacked_message = envelope.unpack(monitoring_messages[1].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 0, 1, topic)

        # third monitoring_message will again have 99 as the message_count
        unpacked_message = envelope.unpack(monitoring_messages[2].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 99, 2, topic)

    def test_monitoring_system_same_topic_different_timestamp_messages(
        self,
        topic,
        payload,
        producer,
        envelope
    ):
        # list of timestamps for which message will be created and published for
        # testing purposes
        timestamp_list = [100, 654, 2010, 2015, 2050]
        with capture_new_messages(topic) as get_messages, \
                capture_new_messages('message-monitoring-log') as get_monitoring_messages:
            for timestamp in timestamp_list:
                producer.publish(self.create_message_with_specified_timestamp(topic, payload, timestamp))
            producer.flush()
            producer.monitoring_message.flush_buffered_info()
            monitoring_messages = get_monitoring_messages()
            messages = get_messages()

        assert len(messages) == 5
        assert len(monitoring_messages) == 4

        # the first monitoring_message should have count as 1
        unpacked_message = envelope.unpack(monitoring_messages[0].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 1, 0, topic)

        # the second monitoring_message should have count as 1
        unpacked_message = envelope.unpack(monitoring_messages[1].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 1, 1, topic)

        # the third monitoring_message should have count as 0
        # since no messages are published with the timestamp between 1200-1800
        unpacked_message = envelope.unpack(monitoring_messages[2].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 0, 2, topic)

        # the forth message should have message count as 3
        unpacked_message = envelope.unpack(monitoring_messages[3].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 3, 3, topic)

    def test_monitoring_system_different_topic_different_timestamp_messages(
        self,
        topic,
        topic_1,
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
        with capture_new_messages('message-monitoring-log') as get_monitoring_messages, \
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

        # varifying contents of the published monitoring messages
        # the first message should be for topic and should have count as 0
        # since no messages of topic 'topic' were published in timeslot 0 - 600
        unpacked_message = envelope.unpack(monitoring_messages[0].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 0, 0, topic)

        # the second message should be for topic-1 and should also have count as 0
        # since no messages of topic 'topic-1' were published in timeslot 0 - 600
        unpacked_message = envelope.unpack(monitoring_messages[1].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 0, 0, topic_1)

        # the third message should be for topic-1 and should have count as 3
        # since 3 messages of topic 'topic-1' were published in timeslot 600 - 1200
        unpacked_message = envelope.unpack(monitoring_messages[2].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 3, 1, topic_1)

        # the forth message should be for topic-1 and should also have count as 0
        # since 0 message of topic 'topic-1' was published in timeslot 1200-1800
        unpacked_message = envelope.unpack(monitoring_messages[3].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 0, 2, topic_1)

        # the fifth message should be for topic with message_count as 2
        # since 2 messages of topic 'topic' were published in timeslot 600 - 1200
        # this message is published as a part of flushing the monitoring message
        # but since this happens in ascending order, topic message is published before
        unpacked_message = envelope.unpack(monitoring_messages[4].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 2, 1, topic)

        # the sixth message should be for topic-1 with message_count as 1
        # this is also published when the flush method of monitoring message is called
        unpacked_message = envelope.unpack(monitoring_messages[5].message.value)
        self.assert_monitoring_system_checks(unpacked_message, 1, 3, topic_1)

    def test_basic_publish_lazy_message(
        self,
        topic,
        lazy_message,
        producer,
        envelope
    ):
        self.test_basic_publish(topic, lazy_message, producer, envelope)

    def test_basic_publish(self, topic, message, producer, envelope):
        with capture_new_messages(topic) as get_messages:
            producer.publish(message)
            producer.flush()

            messages = get_messages()

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
