from __future__ import absolute_import

import logging

from kafka import KafkaClient


logger = logging.getLogger('data_pipeline_clientlib')


def get_kafka_client():
    """Handles building a Kafka connection.  By default, this will connect to
    the Kafka instance in the included docker-compose file.

    TODO: This needs to read from configs.
    """
    return KafkaClient("169.254.255.254:49255")
