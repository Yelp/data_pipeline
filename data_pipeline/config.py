# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from cached_property import cached_property
from kafka import KafkaClient
from swaggerpy import client


class Config(object):

    @cached_property
    def logger(self):
        return logging.getLogger('data_pipeline_clientlib')

    @property
    def kafka_client(self):
        """Handles building a Kafka connection.  By default, this will connect to
        the Kafka instance in the included docker-compose file.

        TODO(DATAPIPE-154|justinc) This should be configured with staticconf
        """
        return KafkaClient("169.254.255.254:49255")

    @property
    def schematizer_client(self):
        """ Returns a swagger-py client for the schematizer api.

        Currently this assumes schematizer is running in local docker as per
        instructions in https://pb.yelpcorp.com/135876
        TODO(DATAPIPE-154|joshszep) This should be configured with staticconf
        """
        return client.get_client(
            "http://localhost:8888/api-docs"
        )

_config = Config()


def get_config():
    return _config
