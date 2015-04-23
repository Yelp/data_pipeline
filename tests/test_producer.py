# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager
from kafka import KafkaClient
from kafka import SimpleConsumer
from kafka.common import KafkaUnavailableError
import mock
import pytest
import subprocess
import time

from data_pipeline.producer import TopicAndMessage
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from data_pipeline.producer import Producer
import data_pipeline.producer as producer


def create_fake_topic(fake_kafka, topic):
    if not isinstance(topic, str):
        raise ValueError("topic must be a str, it cannot be unicode")
    subprocess.call([
        "docker",
        "exec",
        "-i",
        "datapipeline_kafka_1",
        "bash",
        "-c",
        "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper zk:2181 --replication-factor 1 --partition 1 --topic {0}".format(topic)
    ])
    fake_kafka.ensure_topic_exists(topic, timeout=30)
    assert fake_kafka.has_metadata_for_topic(topic)


@contextmanager
def capture_new_messages(topic):
    kafka = producer.get_kafka_client()
    group = str('data_pipeline_clientlib_test')
    consumer = SimpleConsumer(kafka, group, topic)
    consumer.seek(0, 2)  # seek to tail

    def get_messages(count=100):
        return consumer.get_messages(count=count)

    yield get_messages


class TestProducer(object):
    @pytest.yield_fixture(scope='module')
    def fake_kafka(self):
        subprocess.Popen(["docker-compose", "up", "kafka", "zookeeper"])

        def get_connection(timeout=300):
            end_time = time.time() + timeout
            while end_time > time.time():
                try:
                    return KafkaClient("169.254.255.254:49155")
                except KafkaUnavailableError:
                    time.sleep(0.5)
            raise KafkaUnavailableError()

        with mock.patch.object(producer, 'get_kafka_client') as client_mock:
            client = get_connection()
            client_mock.return_value = client
            yield client
        subprocess.call(["docker-compose", "kill"])
        subprocess.call(["docker-compose", "rm", "--force"])

    @pytest.yield_fixture(params=[
        (False, False),
        (False, True),
        (True, False),
        (True, True)
    ])
    def producer(self, request, fake_kafka):
        async, use_work_pool = request.param
        with Producer(async=async, use_work_pool=use_work_pool) as producer:
            yield producer

    @pytest.fixture(scope='module')
    def topic(self, fake_kafka):
        topic = str('my-topic')
        create_fake_topic(fake_kafka, topic)
        return topic

    @pytest.fixture
    def message(self):
        return Message(10, bytes(10), MessageType.create)

    @pytest.fixture
    def topic_and_message(self, topic, message):
        return TopicAndMessage(topic=topic, message=message)

    def test_basic_publish(self, topic_and_message, producer):
        with capture_new_messages(topic_and_message.topic) as get_messages:
            producer.publish(topic_and_message)
            producer.publish(topic_and_message)
            producer.flush()
            messages = get_messages()

        assert len(messages) == 2
