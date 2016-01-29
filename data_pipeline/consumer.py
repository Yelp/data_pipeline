# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from time import time

from kafka.common import ConsumerTimeout
from yelp_kafka.consumer_group import KafkaConsumerGroup

from data_pipeline.base_consumer import BaseConsumer
from data_pipeline.config import get_config
from data_pipeline.message import create_from_kafka_message

logger = get_config().logger


class Consumer(BaseConsumer):
    """The Consumer uses an iterator to get messages that need to be consumed
    from Kafka.

    Note:
        The Consumer leverages the yelp_kafka ``KafkaConsumerGroup``.

    Example:
        with Consumer(
            consumer_name='my_consumer',
            topic_to_consumer_topic_state_map={'topic_a': None, 'topic_b': None}
        ) as consumer:
            while True:
                message = consumer.get_message()
                if message is not None:
                    ... do stuff with message ...
                    consumer.commit_message(message)
    """

    def _start(self):
        self.consumer_group = KafkaConsumerGroup(
            topics=self.topic_to_consumer_topic_state_map.keys(),
            config=self._kafka_consumer_config
        )
        self.consumer_group.start()

    def _stop(self):
        self.consumer_group.stop()

    def get_messages(
            self,
            count,
            blocking=False,
            timeout=get_config().consumer_get_messages_timeout_default
    ):
        messages = []
        has_timeout = timeout is not None
        if has_timeout:
            max_time = time() + timeout
        while len(messages) < count:
            try:
                default_iter_timeout = self.consumer_group.iter_timeout
                # Converting seconds to milliseconds
                self.consumer_group.iter_timeout = timeout * 1000
                message = self.consumer_group.next()
            except ConsumerTimeout:
                break
            finally:
                self.consumer_group.iter_timeout = default_iter_timeout
            message = create_from_kafka_message(
                message.topic,
                message,
                self.force_payload_decode
            )
            self._update_topic_map(message)
            messages.append(message)
            if not blocking or (has_timeout and time() > max_time):
                break
        return messages
