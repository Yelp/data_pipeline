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
            consumer_name (str): See parameter `client_name` in
                :class:`data_pipeline.client.Client`.  The `consumer_name` will
                be registered with Kafka to commit offsets.
            team_name (str): See parameter `team_name` in
                :class:`data_pipeline.client.Client`.
            expected_frequency_seconds (int, ExpectedFrequency): See parameter
                `expected_frequency_seconds` in :class:`data_pipeline.client.Client`.
            topic_to_consumer_topic_state_map ({str:Optional(ConsumerTopicState)}):
                A map of topic names to ``ConsumerTopicState`` objects which
                define the offsets to start from. These objects may be `None`,
                in which case the committed kafka offset for the consumer_name is
                used. If there is no committed kafka offset for the consumer_name
                the MultiprocessingConsumer will begin from the `auto_offset_reset` offset in
                the topic.
            max_buffer_size (int): Maximum size for the internal
                ``multiprocessing.Queue`` used as a shared buffer among all
                ``KafkaConsumerWorker`` objects created internally. The size
                is in number of messages, so it is expected that users may wish
                to tune this amount depending on the expected message memory
                footprint they will be consuming. The larger this buffer, the
                more memory will be consumed, but the more likely that you will
                be able to continually retrieve from `get_messages(..)` without
                encountering blocking.
            auto_offset_reset (str): Used for offset validation. If 'largest'
                reset the offset to the latest available message (tail). If
                'smallest' reset from the earliest (head).
            partitioner_cooldown (float): Waiting time (in seconds) for the
                consumer to acquire the partitions. See
                yelp_kafka/yelp_kafka/partitioner.py for more details
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
        self.max_buffer_size = max_buffer_size
        self.worker_min_sleep_time = worker_min_sleep_time
        self.worker_max_sleep_time = worker_max_sleep_time
        self.running = False
        self.consumer_group = None
        self.consumer_group_thread = None
        self.message_buffer = None

    def start(self):
        """ Start the MultiprocessingConsumer. Normally this should NOT be called directly,
        rather the MultiprocessingConsumer should be used as a context manager, which will
        start automatically when the context enters.
        """
        logger.info("Starting MultiprocessingConsumer '{0}'...".format(self.client_name))
        if self.running:
            raise RuntimeError("MultiprocessingConsumer '{0}' is already running".format(
                self.client_name
            ))

        self._commit_topic_map_offsets()

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
        self.running = True
        logger.info("MultiprocessingConsumer '{0}' started".format(self.client_name))

    def stop(self):
        """ Stop the MultiprocessingConsumer. Normally this should NOT be called directly,
        rather the MultiprocessingConsumer should be used as a context manager, which will
        stop automatically when the context exits.
        """
        logger.info("Stopping MultiprocessingConsumer '{0}'...".format(self.client_name))
        if self.running:
            self.consumer_group.stop_group()
            self.consumer_group_thread.join()
        self.running = False
        logger.info("MultiprocessingConsumer '{0}' stopped".format(self.client_name))

    def __iter__(self):
        while True:
            yield self.get_message(
                blocking=True,
                timeout=get_config().consumer_get_messages_timeout_default
            )

    def get_messages(
            self,
            count,
            blocking=False,
            timeout=get_config().consumer_get_messages_timeout_default
    ):
        """ Retrieve a list of messages from the message buffer, optionally
        blocking if the buffer is depleted.

        Warning:
            If `blocking` is True and `timeout` is None this will block until
            the requested number of messages is retrieved, potentially blocking
            forever. Please be absolutely sure this is what you are intending
            if you use these options!

        Args:
            count (int): Number of messages to retrieve
            blocking (boolean): Set to True to block while waiting for messages
                if the buffer has been depleted. Otherwise returns immediately
                if the buffer reaches depletion.
            timeout (double): Maximum time (in seconds) to wait if blocking is
                set to True. Set to None to wait indefinitely.

        Returns:
            ([data_pipeline.message.Message]): List of Message objects with a
                of maximum size `count`, but may be smaller or empty depending
                on how many messages were retrieved within the timeout.
        """
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
