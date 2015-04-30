import logging
import subprocess
import time
from contextlib import contextmanager
from kafka.common import KafkaUnavailableError
from kafka import KafkaClient
from kafka import SimpleConsumer

import data_pipeline.producer


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
        return self.get_connection

    def __exit__(self, type, value, traceback):
        if not self.kafka_already_running:
            self._stop_kafka()
        return False  # Don't Supress Exception

    def _is_kafka_already_running(self):
        process_up_result = subprocess.call("docker-compose ps kafka | grep Up", shell=True)
        return int(process_up_result) == 0

    def _start_kafka(self):
        print "Starting Kafka"
        self._stop_kafka()
        subprocess.Popen(["docker-compose", "up", "kafka"])

    def _stop_kafka(self):
        subprocess.call(["docker-compose", "kill"])
        subprocess.call(["docker-compose", "rm", "--force"])

    def get_connection(self, timeout=15):
        print "Getting Connection"
        end_time = time.time() + timeout
        while end_time > time.time():
            try:
                return KafkaClient("169.254.255.254:49255")
            except KafkaUnavailableError:
                time.sleep(0.1)
        raise KafkaUnavailableError()


def create_kafka_docker_topic(kafka_docker, topic):
    """This method execs in the docker container because it's the only way to
    control how the topic is created.
    """
    logging.info("Creating Fake Topic")
    if not isinstance(topic, str):
        raise ValueError("topic must be a str, it cannot be unicode")
    subprocess.call([
        "docker",
        "exec",
        "datapipeline_kafka_1",
        "bash",
        "-c",
        "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper zk:2181 --replication-factor 1 --partition 1 --topic {0}".format(topic)
    ])
    logging.info("Waiting for topic")
    kafka_docker.ensure_topic_exists(topic, timeout=5)
    logging.info("Topic Exists")
    assert kafka_docker.has_metadata_for_topic(topic)


@contextmanager
def capture_new_messages(topic):
    """Seeks to the tail of the topic then returns a function that can
    consume messages from that point.
    """
    kafka = data_pipeline.producer.get_kafka_client()
    group = str('data_pipeline_clientlib_test')
    consumer = SimpleConsumer(kafka, group, topic)
    consumer.seek(0, 2)  # seek to tail, 0 is the offset, and 2 is the tail

    def get_messages(count=100):
        return consumer.get_messages(count=count)

    yield get_messages
