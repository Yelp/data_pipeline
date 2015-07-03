# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Queue
from Queue import Empty
from threading import Thread
from time import time

from kafka.common import OffsetCommitRequest
from kafka.util import kafka_bytestring
from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer_group import MultiprocessingConsumerGroup

from data_pipeline._kafka_consumer_worker import KafkaConsumerWorker
from data_pipeline.client import Client
from data_pipeline.config import get_config


logger = get_config().logger


class ConsumerTopicState(object):
    def __init__(self, partition_offset_map, latest_schema_id):
        """Object which holds the state of consumer topic.

        Args:
            partition_offset_map ({int:int}): map of partitions to their last
                seen offsets.
            latest_schema_id: The last seen schema_id.
        """
        self.partition_offset_map = partition_offset_map
        self.latest_schema_id = latest_schema_id


class Consumer(Client):
    """The Consumer deals with buffering messages that need to be consumed
    from Kafka.

    Note:
        The Consumer leverages the yelp_kafka ``MultiprocessingConsumerGroup``.

    Example:
        with Consumer(
            group_id='my_group',
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
            group_id,
            topic_to_consumer_topic_state_map,
            max_buffer_size=get_config().consumer_max_buffer_size_default,
            decode_payload_in_workers=True,
            auto_offset_reset='smallest'):
        """ Creates the Consumer object

        Args:
            group_id (str): The name of the consumer to register with Kafka for
                offset commits.
            topic_to_consumer_topic_state_map ({str:Optional(ConsumerTopicState)}):
                A map of topic names to ``ConsumerTopicState`` objects which
                define the offsets to start from. These objects may be `None`,
                in which case the committed kafka offset for the group_id is
                used. If there is no committed kafka offset for the group_id
                the Consumer will begin from the `auto_offset_reset` offset in
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
        """
        self.group_id = group_id
        self.max_buffer_size = max_buffer_size
        self.topic_to_consumer_topic_state_map = topic_to_consumer_topic_state_map
        self.running = False
        self.consumer_group = None
        self.consumer_group_thread = None
        self.message_buffer = None
        self.decode_payload_in_workers = decode_payload_in_workers
        self.auto_offset_reset = auto_offset_reset

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.stop()
        except:
            logger.exception("Failed to stop the Consumer.")
            if exc_type is not None:
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
        logger.info("Starting Consumer '{0}'...".format(self.group_id))
        if self.running:
            raise RuntimeError("Consumer '{0}' is already running".format(
                self.group_id
            ))

        self._commit_topic_map_offsets()

        self.message_buffer = Queue(self.max_buffer_size)
        self.consumer_group = MultiprocessingConsumerGroup(
            topics=self.topic_to_consumer_topic_state_map.keys(),
            config=KafkaConsumerConfig(
                group_id=self.group_id,
                cluster=get_config().cluster_config,
                auto_offset_reset=self.auto_offset_reset,
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
        logger.info("Consumer '{0}' started".format(self.group_id))

    def stop(self):
        """ Stop the Consumer. Normally this should NOT be called directly,
        rather the Consumer should be used as a context manager, which will
        stop automatically when the context exits.
        """
        logger.info("Stopping consumer '{0}'...".format(self.group_id))
        if self.running:
            self.consumer_group.stop_group()
            self.consumer_group_thread.join()
        self.running = False
        logger.info("Consumer '{0}' stopped".format(self.group_id))

    def __iter__(self):
        while True:
            yield self.get_message(blocking=True, timeout=None)

    def get_message(
            self,
            blocking=False,
            timeout=get_config().consumer_get_messages_timeout_default
    ):
        """ Retrieve a single message from the message buffer, optionally
        blocking if the buffer is depleted. Returns None if no message could
        be retrieved within the timeout.

        Args:
            blocking (boolean): Set to True to block while waiting for messages
                if the buffer has been depleted. Otherwise returns immediately
                if the buffer reaches depletion.
            timeout (double): Maximum time (in seconds) to wait if blocking is
                set to True. Set to None to wait indefinitely.

        Returns:
            (Optional(data_pipeline.message.Message)): Message object or None
                if no message could be retrieved.
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
            timeout=get_config().consumer_get_messages_timeout_default
    ):
        """ Retrieve a list of messages from the message buffer, optionally
        blocking if the buffer is depleted.

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

    def commit_message(self, message):
        """ Commit the offset information of a message to Kafka. Until a message
        is committed the stored kafka offset for this `group_id` is not updated.

        Recommended to avoid calling this too frequently, as it is relatively
        expensive.

        Args:
            message (Message): The message to commit
        """
        return self.commit_messages([message])

    def commit_messages(self, messages):
        """ Commit the offset information of a list of messages to Kafka. Until
        a message is committed the stored kafka offset for this `group_id` is
        not updated.

        Recommended to avoid calling this too frequently, as it is relatively
        expensive.

        If more than one of the given messages share the same `topic/partition`
        combination then the message with the highest offset is committed.

        Note:
            It's recommended to retrieve messages in batches via
            `get_messages(..)`, do your work with them, and then commit them as
            a group with a single call to `commit_messages(..)`

        Args:
            messages [data_pipeline.message.Message]: List of messages to commit
        """
        topic_to_partition_offset_map = {}
        for message in messages:
            pos_info = message.kafka_position_info
            partition_offset_map = topic_to_partition_offset_map.get(message.topic, {})
            max_offset = partition_offset_map.get(
                pos_info.partition,
                0
            )
            if pos_info.offset > max_offset:
                max_offset = pos_info.offset
            partition_offset_map[pos_info.partition] = max_offset
            topic_to_partition_offset_map[message.topic] = partition_offset_map

        return self._send_offset_commit_requests(
            offset_commit_request_list=[
                OffsetCommitRequest(
                    topic=kafka_bytestring(topic),
                    partition=partition,
                    offset=offset,
                    metadata=None
                ) for topic, partition_map in topic_to_partition_offset_map.iteritems()
                for partition, offset in partition_map.iteritems()
            ]
        )

    def reset_topics(self, topic_to_consumer_topic_state_map):
        """ Stop and restart the Consumer with a new
        topic_to_consumer_topic_state_map, returning the state of previous
        topic_to_consumer_topic_state_map.

        Example:
            with Consumer('example', {'topic1': None}) as consumer:
                while True:
                    messages = consumer.get_messages(
                        count=batch_size,
                        blocking=True,
                        timeout=batch_timeout
                    )
                    process_messages(messages)
                    if no__new_topics():
                        continue
                    consumer.commit_messages(messages)
                    topic_map = consumer.topic_to_consumer_topic_state_map
                    new_topics = get_new_topics()
                    for topic in new_topics:
                        if topic not in topic_map:
                            topic_map[topic] = None
                    consumer.reset_topics(topic_map)

        Notes:
            This is an expensive operation, roughly equivalent to destroying
            and recreating the Consumer, so make sure you only are calling this
            when absolutely necessary.

            It's also important to note that you should probably be calling
            `commit_messages` just prior to calling this, otherwise the offsets
            may be lost for the topics you were previously tailing.

        Args:
            topic_to_consumer_topic_state_map ({str:Optional(ConsumerTopicState)}):
                A map of topic names to ``ConsumerTopicState`` objects which
                define the offsets to start from. These objects may be `None`,
                in which case the committed kafka offset for the group_id is
                used. If there is no committed kafka offset for the group_id
                the Consumer will begin from the `auto_offset_reset` offset in
                the topic.

        Returns:
            ({str:ConsumerTopicState}): The previous topic_to_consumer_topic_state_map
        """

        self.stop()
        previous_topic_map = self.topic_to_consumer_topic_state_map
        self.topic_to_consumer_topic_state_map = topic_to_consumer_topic_state_map
        self.start()
        return previous_topic_map

    def _update_topic_map(self, message):
        consumer_topic_state = self.topic_to_consumer_topic_state_map.get(message.topic)
        if consumer_topic_state is None:
            consumer_topic_state = ConsumerTopicState(
                partition_offset_map={},
                latest_schema_id=message.schema_id
            )
        consumer_topic_state.latest_schema_id = message.schema_id
        consumer_topic_state.partition_offset_map[
            message.kafka_position_info.partition
        ] = message.kafka_position_info.offset
        self.topic_to_consumer_topic_state_map[message.topic] = consumer_topic_state

    def _commit_topic_map_offsets(self):
        offset_requests = []
        for topic, consumer_topic_state in self.topic_to_consumer_topic_state_map.iteritems():
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
            get_config().kafka_client.send_offset_commit_request(
                group=kafka_bytestring(self.group_id),
                payloads=offset_commit_request_list
            )
