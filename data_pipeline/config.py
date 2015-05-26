# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from kafka import KafkaClient
from swaggerpy import client
from data_pipeline.schema_cache import SchemaCache

logger = logging.getLogger('data_pipeline_clientlib')


def get_kafka_client():
    """Handles building a Kafka connection.  By default, this will connect to
    the Kafka instance in the included docker-compose file.

    TODO(DATAPIPE-154|justinc) This should be configured with staticconf
    """
    return KafkaClient("169.254.255.254:49255")


def get_schematizer_client():
    return client.get_client(
        # TODO: configurable schematizer swagger-py client URL
        "http://srv1-uswest1adevc:31024/api-docs"
    )


def get_schema_cache():
    return SchemaCache(schematizer_client=get_schematizer_client())