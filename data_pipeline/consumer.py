# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

from kafka.common import OffsetCommitRequest
from kafka.util import kafka_bytestring
from yelp_kafka.config import KafkaConsumerConfig

from data_pipeline.client import Client
from data_pipeline.config import get_config
from data_pipeline.message import Message


logger = get_config().logger


class ConsumerTopicState(object):
    def __init__(self, partition_offset_map, last_seen_schema_id):
        """Object which holds the state of consumer topic.

        Args:
            partition_offset_map ({int:int}): map of partitions to their last
                seen offsets.
            last_seen_schema_id: The last seen schema_id.
        """
        self.partition_offset_map = partition_offset_map
        self.last_seen_schema_id = last_seen_schema_id

    def __repr__(self):
        return '<ConsumerTopicState(last_seen_schema_id: {0}, partition_offset_map: {1})>'.format(
            self.last_seen_schema_id,
            self.partition_offset_map
        )


class Consumer(Client):
    """This is the base interface for the MultiprocessingConsumer and KafkaConsumer
    implementations. It provides a set of methods common to the derived consumer classes
    and specifies which methods should be overriden."""

    def __init__(
        self,
        consumer_name,
        team_name,
        expected_frequency_seconds,
        topic_to_consumer_topic_state_map,
        force_payload_decode=True,
        auto_offset_reset='smallest',
        partitioner_cooldown=get_config().consumer_partitioner_cooldown_default,
    ):
        super(Consumer, self).__init__(
            consumer_name,
            team_name,
            expected_frequency_seconds,
            monitoring_enabled=False
        )
        self.topic_to_consumer_topic_state_map = topic_to_consumer_topic_state_map
        self.force_payload_decode = force_payload_decode
        self.auto_offset_reset = auto_offset_reset
        self.partitioner_cooldown = partitioner_cooldown

    @property
    def client_type(self):
        """String identifying the client type."""
        return "consumer"

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

        Note:
            The dervied class must implement this method.
        """
        raise NotImplementedError

    def stop(self):
        """ Stop the Consumer. Normally this should NOT be called directly,
        rather the Consumer should be used as a context manager, which will
        stop automatically when the context exits.

        Note:
            The dervied class must implement this method.
        """
        raise NotImplementedError

    def __iter__(self):
        while True:
            yield self.get_message(
                blocking=True,
                timeout=get_config().consumer_get_messages_timeout_default
            )

    def get_message(
            self,
            blocking=False,
            timeout=get_config().consumer_get_messages_timeout_default
    ):
        """ Retrieve a single message. Returns None if no message could
        be retrieved within the timeout.

        Warning:
            If `blocking` is True and `timeout` is None this will block until
            a message is retrieved, potentially blocking forever. Please be
            absolutely sure this is what you are intending if you use these
            options!

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

    def get_messages(self):
        """ Retrieve a list of messages from the message buffer, optionally
        blocking until the requested number of messages has been retrieved.

        Note:
            The dervied class must implement this method.

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
        raise NotImplementedError

    def commit_message(self, message):
        """ Commit the offset information of a message to Kafka. Until a message
        is committed the stored kafka offset for this `consumer_name` is not updated.

        Recommended to avoid calling this too frequently, as it is relatively
        expensive.

        Args:
            message (data_pipeline.message.Message): The message to commit
        """
        return self.commit_messages([message])

    def commit_messages(self, messages):
        """ Commit the offset information of a list of messages to Kafka. Until
        a message is committed the stored kafka offset for this `consumer_name`
        is not updated.

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
            max_offset = partition_offset_map.get(pos_info.partition, 0)

            partition_offset_map[pos_info.partition] = max(pos_info.offset, max_offset) + 1
            topic_to_partition_offset_map[message.topic] = partition_offset_map
        self.commit_offsets(topic_to_partition_offset_map)

    def commit_offsets(self, topic_to_partition_offset_map):
        """Commits offset information to kafka.  Allows lower-level control for
        committing offsets.  In general, :meth:`commit_message` or
        :meth:`commit_messages` should be used, but this can be useful when paired with
        :meth:`data_pipeline.position_data.PositionData.topic_to_last_position_info_map`.

        Example::
            The `topic_to_partition_offset_map` should be formatted like::

                {
                  'topic1': {0: 83854, 1: 8943892},
                  'topic2': {0: 190898}
                }

        Args::
            topic_to_partition_offset_map (Dict[str, Dict[int, int]]): Maps from
                topics to a partition and offset map for each topic.
        """
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

    @contextmanager
    def ensure_committed(self, messages):
        """ Context manager which calls `commit_messages` on exit, useful for
        ensuring a group of messages are committed even if an error is
        encountered.


        Example:
            for msg in consumer:
                if msg is None:
                    continue
                with consumer.ensure_committed(msg):
                    do_things_with_message(msg)

        Example:
            with consumer.ensure_committed(consumer.get_messages()) as messages:
                do_things_with_messages(messages)

        Args:
            messages (list[data_pipeline.message.Message]): Messages to commit
                when this context manager exits. May also pass a single
                `data_pipeline.message.Message` object.
        """
        try:
            yield messages
        finally:
            if isinstance(messages, Message):
                messages = [messages]
            self.commit_messages(messages=messages)

    def reset_topics(self, topic_to_consumer_topic_state_map):
        """ Stop and restart the Consumer with a new
        topic_to_consumer_topic_state_map. This can be used to change topics
        which are being consumed and/or modifying the offsets of the topics
        already being consumed.

        Example:
            with Consumer('example', {'topic1': None}) as consumer:
                while True:
                    messages = consumer.get_messages(
                        count=batch_size,
                        blocking=True,
                        timeout=batch_timeout
                    )
                    process_messages(messages)
                    if no_new_topics():
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
                in which case the committed kafka offset for the consumer_name is
                used. If there is no committed kafka offset for the consumer_name
                the Consumer will begin from the `auto_offset_reset` offset in
                the topic.
        """
        self.stop()
        self.topic_to_consumer_topic_state_map = topic_to_consumer_topic_state_map
        self.start()

    def _update_topic_map(self, message):
        consumer_topic_state = self.topic_to_consumer_topic_state_map.get(message.topic)
        if consumer_topic_state is None:
            consumer_topic_state = ConsumerTopicState(
                partition_offset_map={},
                last_seen_schema_id=message.schema_id
            )
        consumer_topic_state.last_seen_schema_id = message.schema_id
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
                group=kafka_bytestring(self.client_name),
                payloads=offset_commit_request_list
            )

    @property
    def _kafka_consumer_config(self):
        """ The ``KafkaConsumerConfig`` for the Consumer.

        Notes:
            This is not a ``@cached_property`` since there is the possibility
            that the cluster_config could change during runtime and users could
            leverage this for responding to topology changes.

            ``auto_commit`` is set to False to ensure clients can determine when
            they want their topic offsets committed via commit_messages(..)
        """
        return KafkaConsumerConfig(
            group_id=self.client_name,
            cluster=get_config().cluster_config,
            auto_offset_reset=self.auto_offset_reset,
            auto_commit=False,
            partitioner_cooldown=self.partitioner_cooldown
        )
