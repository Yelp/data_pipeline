# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from kafka import KafkaClient


logger = logging.getLogger('data_pipeline_clientlib')


def get_kafka_client():
    """Handles building a Kafka connection.  By default, this will connect to
    the Kafka instance in the included docker-compose file.

    TODO(DATAPIPE-154|justinc) This should be configured with staticconf
    """
    return KafkaClient("169.254.255.254:49255")
