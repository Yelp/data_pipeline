# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from cached_property import cached_property
from kafka import KafkaClient
from swaggerpy import client
from yelp_kafka.config import ClusterConfig

# TODO(DATAPIPE-154|justinc) Everything here should be configured with staticconf


class Config(object):

    @cached_property
    def logger(self):
        return logging.getLogger('data_pipeline_clientlib')

    @property
    def kafka_client(self):
        """Handles building a Kafka connection.  By default, this will connect to
        the Kafka instance in the included docker-compose file.
        """
        return KafkaClient(self.cluster_config.broker_list)

    @property
    def schematizer_client(self):
        """ Returns a swagger-py client for the schematizer api.

        Currently this assumes schematizer is running in local docker as per
        instructions in https://pb.yelpcorp.com/135876
        """
        return client.get_client(
            'http://localhost:8888/api-docs'
        )

    @property
    def cluster_config(self):
        """ Returns a yelp_kafka.config.ClusterConfig
        """
        return ClusterConfig(
            name='data_pipeline',
            broker_list=['169.254.255.254:49255'],
            zookeeper='169.254.255.254:32796'
        )

    @property
    def consumer_max_buffer_size_default(self):
        """ Maximum queue size for Consumer objects
        """
        return 1000

    @property
    def consumer_get_messages_timeout_default(self):
        """ Default timeout for blocking calls to ``Consumer.get_messages``
        """
        return 0.1

    @property
    def consumer_partitioner_cooldown_default(self):
        """ Default partitioner cooldown time. See ``yelp_kafka.partitioner`` for
        more details.
        """
        return 0.5

    @property
    def consumer_worker_min_sleep_time_default(self):
        """ Default ``KafkaConsumerWorker`` sleep time minimum. Must be lower
        than the maximum sleep time.
        """
        return 0.1

    @property
    def consumer_worker_max_sleep_time_default(self):
        """ Default ``KafkaConsumerWorker`` sleep time maximum. Must be higher
        than the minimum sleep time.
        """
        return 0.2

_config = Config()


def get_config():
    return _config
