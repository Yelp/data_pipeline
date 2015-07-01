# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing

import mock
import pytest

from data_pipeline import lazy_message
from data_pipeline.async_producer import AsyncProducer
from data_pipeline.message_type import MessageType
from data_pipeline.producer import Producer
from tests.helpers.kafka_docker import capture_new_messages
from tests.helpers.kafka_docker import create_kafka_docker_topic
from tests.helpers.kafka_docker import setup_capture_new_messages_consumer


class RandomException(Exception):
    pass


class TestProducer(object):
    @pytest.fixture(params=[
        (Producer, False),
        (Producer, True),
        (AsyncProducer, False),
        (AsyncProducer, True)
    ])
    def producer_instance(self, request, kafka_docker):
        producer_klass, use_work_pool = request.param
        return producer_klass(use_work_pool=use_work_pool)

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope='module')
    def topic(self, topic_name, kafka_docker):
        create_kafka_docker_topic(kafka_docker, topic_name)
        return topic_name

    @pytest.yield_fixture
    def patch_payload(self):
        with mock.patch.object(
            lazy_message.LazyMessage,
            'payload',
            new_callable=mock.PropertyMock
        ) as mock_payload:
            mock_payload.return_value = bytes(7)
            yield mock_payload

    @pytest.fixture
    def lazy_message(self, topic_name):
        return lazy_message.LazyMessage(topic_name, 10, {1: 100}, MessageType.create)

    def test_basic_publish_lazy_message(
        self,
        topic,
        lazy_message,
        patch_payload,
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
        with capture_new_messages(topic) as get_messages:
            with producer_instance as producer:
                producer.publish(message)
                producer.flush()
            assert len(multiprocessing.active_children()) == 0
            assert len(get_messages()) == 1

    def test_messages_published_without_flush(self, topic, message, producer_instance):
        with capture_new_messages(topic) as get_messages:
            with producer_instance as producer:
                producer.publish(message)
            assert len(multiprocessing.active_children()) == 0
            assert len(get_messages()) == 1

    def test_empty_starting_checkpoint_data(self, producer):
        position_data = producer.get_checkpoint_position_data()
        assert position_data.last_published_message_position_info is None
        assert position_data.topic_to_last_position_info_map == {}
        assert position_data.topic_to_kafka_offset_map == {}

    def test_child_processes_do_not_survive_an_exception(self, producer_instance, message):
        with pytest.raises(RandomException):
            with producer_instance as producer:
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
