# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from kafka import KafkaClient
from swaggerpy import client
from yelp_lib.decorators import memoized

from data_pipeline.schema_cache import SchemaCache

logger = logging.getLogger('data_pipeline_clientlib')


def get_kafka_client():
    """Handles building a Kafka connection.  By default, this will connect to
    the Kafka instance in the included docker-compose file.

    TODO(DATAPIPE-154|justinc) This should be configured with staticconf
    """
    return KafkaClient("169.254.255.254:49255")


def get_schematizer_client():
    """ Returns a swagger-py client for the schematizer api.

    Currently this assumes schematizer is running in local docker as per
    instructions in https://pb.yelpcorp.com/135876
    TODO(DATAPIPE-154|joshszep) This should be configured with staticconf
    """
    return client.get_client(
        "http://localhost:8888/api-docs"
    )


@memoized
def get_schema_cache():
    return SchemaCache(get_schematizer_client())
