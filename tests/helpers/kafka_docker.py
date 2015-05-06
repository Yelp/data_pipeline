# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import subprocess
import time
from contextlib import contextmanager

from kafka import KafkaClient
from kafka import SimpleConsumer
from kafka.common import KafkaUnavailableError

from data_pipeline.config import logger


_ONE_MEGABYTE = 1024 * 1024


class KafkaDocker(object):
    """Context manager that gets a connection to an already running kafka docker
    container if available, and if not, runs a new container for the duration
    of tests.
    """

    def __init__(self):
        self.kafka_already_running = self._is_kafka_already_running()

    def __enter__(self):
        if not self.kafka_already_running:
            self._start_kafka()
        else:
            logger.info("Using running Kafka container")
        return KafkaDocker.get_connection

    def __exit__(self, type, value, traceback):
        if not self.kafka_already_running:
            self._stop_kafka()
        return False  # Don't Supress Exception

    def _is_kafka_already_running(self):
        process_up_result = subprocess.call("docker-compose ps kafka | grep Up", shell=True)
        return int(process_up_result) == 0

    def _start_kafka(self):
        self._stop_kafka()
        logger.info("Starting Kafka")
        subprocess.Popen(["docker-compose", "up", "kafka"])

    def _stop_kafka(self):
        logger.info("Stopping Kafka containers")
        subprocess.call(["docker-compose", "kill"])
        subprocess.call(["docker-compose", "rm", "--force"])

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
