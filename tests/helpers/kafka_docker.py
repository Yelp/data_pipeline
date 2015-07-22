# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import subprocess
import time
from contextlib import contextmanager

from kafka import KafkaClient
from kafka import SimpleConsumer
from kafka.common import KafkaUnavailableError

from data_pipeline.config import get_config


_ONE_MEGABYTE = 1024 * 1024
logger = get_config().logger


class KafkaDocker(object):
    """Helper for getting a Kafka Docker connection, which will wait for the
    service to come up.
    """

    @classmethod
    def get_connection(cls, timeout=15):
        end_time = time.time() + timeout
        logger.info("Getting connection to Kafka container on yocalhost")
        while end_time > time.time():
            try:
                return KafkaClient("169.254.255.254:49255")
            except KafkaUnavailableError:
                logger.info("Kafka not yet available, waiting...")
                time.sleep(0.1)
        raise KafkaUnavailableError()


def create_kafka_docker_topic(kafka_docker, topic):
    """This method execs in the docker container because it's the only way to
    control how the topic is created.
    """
    if kafka_docker.has_metadata_for_topic(topic):
        return

    logger.info("Creating Fake Topic")
    if not isinstance(topic, str):
        raise ValueError("topic must be a str, it cannot be unicode")

    kafka_create_topic_command = (
        "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper zk:2181 "
        "--replication-factor 1 --partition 1 --topic {topic}"
    ).format(topic=topic)

    subprocess.call([
        "docker",
        "exec",
        "datapipeline_kafka_1",
        "bash",
        "-c",
        kafka_create_topic_command
    ])

    logger.info("Waiting for topic")
    kafka_docker.ensure_topic_exists(topic, timeout=5)
    logger.info("Topic Exists")
    assert kafka_docker.has_metadata_for_topic(topic)


@contextmanager
def capture_new_messages(topic):
    """Seeks to the tail of the topic then returns a function that can
    consume messages from that point.
    """
    with setup_capture_new_messages_consumer(topic) as consumer:
        def get_messages(count=100):
            return consumer.get_messages(count=count)

        yield get_messages


@contextmanager
def setup_capture_new_messages_consumer(topic):
    """Seeks to the tail of the topic then returns a function that can
    consume messages from that point.
    """
    kafka = KafkaDocker.get_connection()
    group = str('data_pipeline_clientlib_test')
    consumer = SimpleConsumer(kafka, group, topic, max_buffer_size=_ONE_MEGABYTE)
    consumer.seek(0, 2)  # seek to tail, 0 is the offset, and 2 is the tail

    yield consumer
