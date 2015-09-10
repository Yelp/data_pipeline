# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from time import time

from kafka.common import ConsumerTimeout
from yelp_kafka.consumer_group import KafkaConsumerGroup

from data_pipeline.config import get_config
from data_pipeline.message import create_from_kafka_message
from data_pipeline.consumer import Consumer


logger = get_config().logger


class KafkaConsumer(Consumer):
    """The KafkaConsumer uses an iterator to get messages that need to be consumed
    from Kafka.

    Note:
        The KafkaConsumer leverages the yelp_kafka ``KafkaConsumerGroup``.

    Example:
        with KafkaConsumer(
            consumer_name='my_consumer',
            topic_to_consumer_topic_state_map={'topic_a': None, 'topic_b': None}
        ) as consumer:
            while True:
                message = consumer.get_message()
                if message is not None:
                    ... do stuff with message ...
                    consumer.commit_message(message)
    """
    def __init__(
            self,
            consumer_name,
            team_name,
            expected_frequency_seconds,
            topic_to_consumer_topic_state_map,
            force_payload_decode=True,
    ):
        super(KafkaConsumer, self).__init__(
            consumer_name,
            team_name,
            expected_frequency_seconds,
            topic_to_consumer_topic_state_map,
            force_payload_decode
        )

    def _start(self):
        self.consumer_group = KafkaConsumerGroup(
            topics=self.topic_to_consumer_topic_state_map.keys(),
            config=self._kafka_consumer_config
        )
        self.consumer_group.start()

    def _stop(self):
        """ Stop the KafkaConsumer. Normally this should NOT be called directly,
        rather the KafkaConsumer should be used as a context manager, which will
        stop automatically when the context exits.
        """
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
                self.consumer_group.iter_timeout = timeout
                message = self.consumer_group.next()
                self.consumer_group.iter_timeout = default_iter_timeout
            except ConsumerTimeout:
                break
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
