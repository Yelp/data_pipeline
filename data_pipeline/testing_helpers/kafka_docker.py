# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

from kafka import KafkaClient
from kafka import SimpleConsumer

from data_pipeline.config import get_config
from data_pipeline.message import create_from_offset_and_message


_ONE_MEGABYTE = 1024 * 1024
logger = get_config().logger


@contextmanager
def capture_new_data_pipeline_messages(topic):
    """contextmanager that moves to the tail of the given topic, and waits to
    receive new messages, returning a function that can be called zero or more
    times which will retrieve decoded data pipeline messages from the topic.

    Returns:
        Callable[[int], List[Message]]: Function that takes a single
            optional argument, count, and returns up to count decoded data pipeline
            messages.  This function does not block, and will return however many
            messages are available immediately.  Default count is 100.
    """
    with capture_new_messages(topic) as get_kafka_messages:
        def get_data_pipeline_messages(count=100):
            kafka_messages = get_kafka_messages(count)
            return [
                create_from_offset_and_message(kafka_message)
                for kafka_message in kafka_messages
            ]

        yield get_data_pipeline_messages


@contextmanager
def capture_new_messages(topic):
    """Seeks to the tail of the topic then returns a function that can
    consume messages from that point.
    """
    with setup_capture_new_messages_consumer(topic) as consumer:
        def get_messages(count=100):
            return consumer.get_messages(count=count)

        yield get_messages


@contextmanager
def setup_capture_new_messages_consumer(topic):
    """Seeks to the tail of the topic then returns a function that can
    consume messages from that point.
    """
    kafka = KafkaClient(get_config().cluster_config.broker_list)
    group = str('data_pipeline_clientlib_test')
    consumer = SimpleConsumer(kafka, group, topic, max_buffer_size=_ONE_MEGABYTE)
    consumer.seek(0, 2)  # seek to tail, 0 is the offset, and 2 is the tail

    yield consumer

    kafka.close()
