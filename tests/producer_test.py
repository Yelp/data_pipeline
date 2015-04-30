# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.producer import TopicAndMessage
from data_pipeline.producer import AsyncProducer
from data_pipeline.producer import Producer
from tests.helpers.kafka_docker import capture_new_messages
from tests.helpers.kafka_docker import create_kafka_docker_topic


class TestProducer(object):
    @pytest.yield_fixture(params=[
        (Producer, False),
        (Producer, True),
        (AsyncProducer, False),
        (AsyncProducer, True)
    ])
    def producer(self, request, kafka_docker):
        producer_klass, use_work_pool = request.param
        with producer_klass(use_work_pool=use_work_pool) as producer:
            yield producer

    @pytest.fixture(scope='module')
    def topic(self, kafka_docker):
        topic = str('my-topic')
        create_kafka_docker_topic(kafka_docker, topic)
        return topic

    @pytest.fixture
    def topic_and_message(self, topic, message):
        return TopicAndMessage(topic=topic, message=message)

    def test_basic_publish(self, topic_and_message, producer, envelope):
        with capture_new_messages(topic_and_message.topic) as get_messages:
            producer.publish(topic_and_message)
            producer.flush()

            messages = get_messages()

        assert len(messages) == 1
        unpacked_message = envelope.unpack(messages[0].message.value)
        expected_message = topic_and_message.message
        assert unpacked_message['payload'] == expected_message.payload
        assert unpacked_message['schema_id'] == expected_message.schema_id
