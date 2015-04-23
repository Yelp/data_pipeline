from __future__ import absolute_import

from kafka import KafkaClient


def get_kafka_client():
    return KafkaClient("dev36-devc:49155")
