# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from kafka import KafkaClient
from swaggerpy import client


logger = logging.getLogger('data_pipeline_clientlib')


def get_kafka_client():
    """Handles building a Kafka connection.  By default, this will connect to
    the Kafka instance in the included docker-compose file.

    TODO(DATAPIPE-154|justinc) This should be configured with staticconf
    """
    return KafkaClient("169.254.255.254:49255")


def get_schematizer_client():
        # TODO: configurable schematizer swagger-py client URL
        # For now, running schematizer in local docker:
        #   https://pb.yelpcorp.com/135876
    return client.get_client(
        "http://localhost:1210/api-docs"
    )
