# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing

import pytest

from data_pipeline.consumer import Consumer
from data_pipeline.consumer import ConsumerTopicState
from data_pipeline.producer import Producer
from data_pipeline.message import Message
from tests.helpers.kafka_docker import create_kafka_docker_topic


class TestConsumer(object):
    @pytest.fixture()
    def producer_instance(self, kafka_docker):
        return Producer(use_work_pool=False)

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture()
    def consumer_instance(self, topic, kafka_docker):
        return Consumer('test_consumer', {topic: None})

    @pytest.yield_fixture
    def consumer(self, consumer_instance):
        with consumer_instance as consumer:
            yield consumer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope='module')
    def topic(self, topic_name, kafka_docker):
        create_kafka_docker_topic(kafka_docker, topic_name)
        return topic_name

    def test_get_message_none(self, consumer, topic):
        message = consumer.get_message(blocking=False)
        assert message is None
        assert consumer.get_topic_map()[topic] is None

    def test_get_messages_empty(self, consumer, topic):
        messages = consumer.get_messages(count=10, blocking=False)
        assert len(messages) == 0
        assert consumer.get_topic_map()[topic] is None

    def test_basic_publish_retrieve(
            self,
            topic,
            message,
            producer,
            consumer_instance,
            example_payload_data,
            registered_schema
    ):
        producer.publish(message)
        producer.flush()
        with consumer_instance as consumer:
            message = consumer.get_message(blocking=True, timeout=5.0)
            assert isinstance(message, Message)
            assert message.payload == message.payload
            assert message.schema_id == message.schema_id
            assert message.schema_id == registered_schema.schema_id
            assert message.topic == topic
            assert message.payload_data == example_payload_data
            assert consumer.message_buffer.empty()
            topic_state = consumer.get_topic_map()[topic]
            assert isinstance(topic_state, ConsumerTopicState)
            assert topic_state.latest_schema_id == message.schema_id