# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Queue
from Queue import Empty
from threading import Thread
from time import time

from yelp_kafka.consumer_group import MultiprocessingConsumerGroup

from data_pipeline._kafka_consumer_worker import KafkaConsumerWorker
from data_pipeline.config import get_config
from data_pipeline.consumer import Consumer


logger = get_config().logger


class MultiprocessingConsumer(Consumer):
    """The MultiprocessingConsumer deals with buffering messages that need to be consumed
    from Kafka.

    Note:
        The MultiprocessingConsumer leverages the yelp_kafka ``MultiprocessingConsumerGroup``.

    Example:
        with MultiprocessingConsumer(
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
            max_buffer_size=get_config().consumer_max_buffer_size_default,
            worker_min_sleep_time=get_config().consumer_worker_min_sleep_time_default,
            worker_max_sleep_time=get_config().consumer_worker_max_sleep_time_default
    ):
        """ Creates the MultiprocessingConsumer object

        Notes:
            worker_min_sleep_time and worker_max_sleep_time exist (rather than
            a single sleep time) to ensure subprocesses don't always wake up
            at the same time (aka configurable 'jitter').

        Args:
            max_buffer_size (int): Maximum size for the internal
                ``multiprocessing.Queue`` used as a shared buffer among all
                ``KafkaConsumerWorker`` objects created internally. The size
                is in number of messages, so it is expected that users may wish
                to tune this amount depending on the expected message memory
                footprint they will be consuming. The larger this buffer, the
                more memory will be consumed, but the more likely that you will
                be able to continually retrieve from `get_messages(..)` without
                encountering blocking.
            worker_min_sleep_time (float): Minimum time (in seconds) for
                background workers in the consumer to sleep while waiting to
                retry adding messages to the shared `multiprocess.Queue`
            worker_max_sleep_time (float): Maximum time (in seconds) for
                background workers in the consumer to sleep while waiting to
                retry adding messages to the shared `multiprocess.Queue`

        """
        super(MultiprocessingConsumer, self).__init__(
            consumer_name,
            team_name,
            expected_frequency_seconds,
            topic_to_consumer_topic_state_map,
            force_payload_decode
        )
        self.message_buffer = None
        self.consumer_group_thread = None
        self.max_buffer_size = max_buffer_size
        self.worker_min_sleep_time = worker_min_sleep_time
        self.worker_max_sleep_time = worker_max_sleep_time

    def _start(self):
        self.message_buffer = Queue(self.max_buffer_size)
        self.consumer_group = MultiprocessingConsumerGroup(
            topics=self.topic_to_consumer_topic_state_map.keys(),
            config=self._kafka_consumer_config,
            consumer_factory=KafkaConsumerWorker.create_factory(
                message_buffer=self.message_buffer,
                decode_payload=self.force_payload_decode,
                min_sleep_time=self.worker_min_sleep_time,
                max_sleep_time=self.worker_max_sleep_time
            )
        )
        self.consumer_group_thread = Thread(
            target=self.consumer_group.start_group
        )
        self.consumer_group_thread.setDaemon(False)
        self.consumer_group_thread.start()

    def _stop(self):
        self.consumer_group.stop_group()
        self.consumer_group_thread.join()

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
                message = self.message_buffer.get(blocking, timeout)
                self._update_topic_map(message)
                messages.append(message)
            except Empty:
                if not blocking or (has_timeout and time() > max_time):
                    break
        return messages
