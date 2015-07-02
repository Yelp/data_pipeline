# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Queue
from Queue import Empty
from threading import Thread
from time import time

from kafka.common import OffsetCommitRequest
from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer_group import MultiprocessingConsumerGroup

from data_pipeline._kafka_consumer_worker import KafkaConsumerWorker
from data_pipeline.client import Client
from data_pipeline.config import get_config


logger = get_config().logger

CONSUMER_MAX_BUFFER_SIZE_DEFAULT = 1000
CONSUMER_GET_MESSAGES_TIMEOUT_DEFAULT = 0.1


class ConsumerTopicState(object):
    def __init__(self, partition_offset_map, latest_schema_id):
        """
        :param {int:int} partition_offset_map: map of partitions to their last
        seen offsets
        :param latest_schema_id: last seen schema_id
        """
        self.partition_offset_map = partition_offset_map
        self.latest_schema_id = latest_schema_id


class Consumer(Client):
    """The Consumer deals with buffering messages that need to be consumed
    from Kafka.
    The Consumer leverages the yelp_kafka MultiprocessingConsumerGroup.

    Basic usage:
    ````
        with Consumer(
            group_id='my_group',
            topic_map={'topic_a': None, 'topic_b': None}
        ) as consumer:
            while True:
                message = consumer.get_message()
                if message is not None:
                    ... do stuff with message ...
                    consumer.commit_message(message)
    ````
    """

    def __init__(
            self,
            group_id,
            topic_map,
            max_buffer_size=CONSUMER_MAX_BUFFER_SIZE_DEFAULT,
            decode_payload_in_workers=True):
        """ Create the Consumer object

        :param str group_id: the name of the consumer to register with Kafka for
        offset commits.
        :param {str:ConsumerTopicState} topic_map: A map of topic names to the
         ConsumerTopicState that define the offsets to start from (may be None,
         in which case the committed kafka offset is used)
        :param int max_buffer_size: Maximum size for the internal message buffer
        :return:
        """
        self.group_id = group_id
        self.max_buffer_size = max_buffer_size
        self.topic_map = topic_map
        self.running = False
        self.consumer_group = None
        self.consumer_group_thread = None
        self.message_buffer = None
        self.decode_payload_in_workers = decode_payload_in_workers

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.stop()
        except:
            logger.exception("Failed to stop the Consumer.")
            if exc_type is None:
                # The exception shouldn't mask the original exception if there
                # is one, but if an exception occurs, we want it to show up.
                raise
        # Returning any kind of truthy value will suppress the exception, if
        # there was one.  The intention of returning False here is to never
        # suppress the exception.  See:
        # https://docs.python.org/2/reference/datamodel.html#object.__exit__
        # for more information.
        return False

    def start(self):
        """ Start the Consumer. Normally this should NOT be called directly,
        rather the Consumer should be used as a context manager, which will
        start automatically when the context enters.
        """
        logger.info("Starting Consumer...")
        if self.running:
            raise RuntimeError("Consumer is already running")

        self._commit_topic_map_offsets()

        self.message_buffer = Queue(self.max_buffer_size)
        self.consumer_group = MultiprocessingConsumerGroup(
            topics=self.topic_map.keys(),
            config=KafkaConsumerConfig(
                group_id=self.group_id,
                cluster=get_config().cluster_config,
                auto_offset_reset='smallest',
                auto_commit=False,
                partitioner_cooldown=0.5
            ),
            consumer_factory=KafkaConsumerWorker.create_factory(
                message_buffer=self.message_buffer,
                decode_payload=self.decode_payload_in_workers
            )
        )
        self.consumer_group_thread = Thread(
            target=self.consumer_group.start_group
        )
        self.consumer_group_thread.setDaemon(False)
        self.consumer_group_thread.start()
        self.running = True

    def stop(self):
        """ Stop the Consumer. Normally this should NOT be called directly,
        rather the Consumer should be used as a context manager, which will
        stop automatically when the context exits.
        """
        logger.info("Stopping consumer...")
        if self.running:
            self.consumer_group.stop_group()
            self.consumer_group_thread.join()
        logger.info("Consumer stopped")

    def __iter__(self):
        while True:
            yield self.get_message(blocking=True, timeout=None)

    def get_message(
            self,
            blocking=False,
            timeout=CONSUMER_GET_MESSAGES_TIMEOUT_DEFAULT
    ):
        """ Retrieve a single message from the message buffer, optionally
        blocking if the buffer is depleted. Returns None if no message could
        be retrieved within the timeout.

        :param boolean blocking: Set to True to block while waiting for messages
        if the buffer has been depleted. Otherwise returns immediately if the
        buffer reaches depletion.
        :param double timeout: Maximum time (in seconds) to wait if blocking is
        set to True. Set to None to wait indefinitely.
        :rtype data_pipeline.message.Message or None
        """
        return next(iter(
            self.get_messages(
                count=1,
                blocking=blocking,
                timeout=timeout
            )),
            None
        )

    def get_messages(
            self,
            count,
            blocking=False,
            timeout=CONSUMER_GET_MESSAGES_TIMEOUT_DEFAULT
    ):
        """ Retrieve a list of messages from the message buffer, optionally
        blocking if the buffer is depleted.

        :param int count: Number of messages to retrieve
        :param boolean blocking: Set to True to block while waiting for messages
        if the buffer has been depleted. Otherwise returns immediately if the
        buffer reaches depletion.
        :param double timeout: Maximum time (in seconds) to wait if blocking is
        set to True. Set to None to wait indefinitely.
        :rtype [data_pipeline.message.Message]
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

    def commit_message(self, message):
        """ Commit the offset information of a message to Kafka. Call this
        with every message after all work has been completed with it.

        :param data_pipeline.message.Message message: Message to commit
        """
        return self.commit_messages([message])

    def commit_messages(self, messages):
        """ Commit the offset information of a list of messages to Kafka.

        :param [data_pipeline.message.Message] messages: List of messages to
        commit
        """
        return self._send_offset_commit_requests(
            offset_commit_request_list=[
                OffsetCommitRequest(
                    topic=message.topic,
                    partition=message.kafka_position_info.partition,
                    offset=message.kafka_position_info.offset,
                    metadata=None
                ) for message in messages
            ]
        )

    def reset_topics(self, topic_map):
        """ Stop and restart the Consumer with a new topic_map, returning the
        state of previous topic_map.

        NOTE: This is an expensive operation, roughly equivalent to destroying
        and recreating the Consumer, so make sure you only are calling this when
        absolutely necessary.

        :param {str:ConsumerTopicState} topic_map: A map of topic names to the
         ConsumerTopicState that define the offsets to start from (may be None,
         in which case the committed kafka offset is used)
        :return: The previous topic_map
        :rtype {str:ConsumerTopicState}
        """

        self.stop()
        previous_topic_map = self.get_topic_map()
        self.topic_map = topic_map
        self.start()
        return previous_topic_map

    def get_topic_map(self):
        """ Retrieve the current state of the topic_map, which
        can be used to create new Consumer objects or for calls to reset_topics
        """
        return self.topic_map

    def _update_topic_map(self, message):
        """

        :param data_pipeline.message.Message message: message from which to
        update ``self.topic_map``
        :return:
        """
        consumer_topic_state = self.topic_map.get(message.topic)
        if consumer_topic_state is None:
            consumer_topic_state = ConsumerTopicState(
                partition_offset_map={},
                latest_schema_id=message.schema_id
            )
        consumer_topic_state.latest_schema_id = message.schema_id
        consumer_topic_state.partition_offset_map[
            message.kafka_position_info.partition
        ] = message.kafka_position_info.offset
        self.topic_map[message.topic] = consumer_topic_state

    def _commit_topic_map_offsets(self):
        offset_requests = []
        for topic, consumer_topic_state in self.topic_map.iteritems():
            if consumer_topic_state is None:
                continue
            for partition, offset in consumer_topic_state.partition_offset_map.iteritems():
                offset_requests.append(
                    OffsetCommitRequest(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        metadata=None
                    )
                )
        self._send_offset_commit_requests(offset_requests)

    def _send_offset_commit_requests(self, offset_commit_request_list):
        if len(offset_commit_request_list) > 0:
            get_config().kafka_client().send_offset_commit_request(
                group=self.group_id,
                payloads=offset_commit_request_list
            )
