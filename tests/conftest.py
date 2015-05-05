# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import mock
import pytest

from data_pipeline.envelope import Envelope
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from tests.helpers.kafka_docker import KafkaDocker
import data_pipeline._kafka_producer


logging.basicConfig(level=logging.DEBUG, filename='logs/test.log')


@pytest.fixture
def payload():
    return bytes(10)


@pytest.fixture(scope='module')
def topic_name():
    return str('my-topic')


@pytest.fixture
def message(topic_name, payload):
    return Message(topic_name, 10, payload, MessageType.create)


@pytest.fixture
def envelope():
    return Envelope()


@pytest.yield_fixture(scope='session')
def kafka_docker():
    with KafkaDocker() as get_connection:
        with mock.patch.object(data_pipeline._kafka_producer, 'get_kafka_client') as client_mock:
            client = get_connection()
            client_mock.return_value = client
            yield client
