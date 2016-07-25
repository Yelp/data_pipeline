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
    """
    The Consumer uses an iterator to get messages that need to be consumed
    from Kafka.

    Args:
        consumer_name (str): See parameter `client_name` in
            :class:`data_pipeline.client.Client`.  The `consumer_name` will
            be registered with Kafka to commit offsets.
        team_name (str): See parameter `team_name` in
            :class:`data_pipeline.client.Client`.
        expected_frequency_seconds (int, ExpectedFrequency): See parameter
            `expected_frequency_seconds` in :class:`data_pipeline.client.Client`.
        topic_to_consumer_topic_state_map ({str:Optional(ConsumerTopicState)}):
            A map of topic names to `ConsumerTopicState` objects which define
            the offsets to start from. The ConsumerTopicState of a topic may be
            `None`, in which case the committed kafka offset for the
            consumer_name is used. If there is no committed kafka offset for
            the consumer_name the consumer will begin from the
            `auto_offset_reset` offset in the topic.
        consumer_source (ConsumerSource): Object to specify the topics this
            consumer consumes messages from. It must be a
            :class:`data_pipeline.consumer_source.ConsumerSource` object. For
            example, to process messages from a fixed set of topics, use
            :class:`data_pipeline.consumer_source.FixedTopics`.
            In case of FixedSchema consumer source, at most one schema_id can
            be provided per topic. Consumer would use that schema as reader
            schema to decode the message. In case no schema id for a topic is
            specified, then the Consumer would use the schema id that the
            message was encoded with to decode the message.
        auto_offset_reset (str): automatically resets the offset when there is
            no initial offset in Zookeeper or if an offset is out of range.
            If 'largest', reset the offset to the latest available message (tail).
            If 'smallest' reset from the earliest (head).
        partitioner_cooldown (float): Waiting time (in seconds) for the
            consumer to acquire the partitions. See
            yelp_kafka/yelp_kafka/partitioner.py for more details
        pre_rebalance_callback (Optional[Callable[{str:list[int]}, None]]):
            Optional callback which is passed a dict of topic as key and list
            of partitions as value. It's important to note this may be called
            multiple times in a single repartition, so any actions taken as a
            result must be idempotent. You are guaranteed that no messages will
            be consumed between this callback and the post_rebalance_callback.
        post_rebalance_callback (Optional[Callable[{str:list[int]}, None]]):
            Optional callback which is passed a dict of topic as key and list
            of partitions as value which were acquired in a repartition. You
            are guaranteed that no messages will be consumed between the
            pre_rebalance_callback and this callback.
        fetch_offsets_for_topics: (Optional[Callable[List[str],
            Dict[str, Optional[Dict[int, int]]]]]): Optional callback which is
            passed a list of topics, and should return a dictionary where keys
            are topic names and values are either None if no offset should be
            manually set, or a map from partition to offset.
            If implemented, this function will be called every time
            consumer refreshes the topics, so that the consumer can provide a
            map of partitions to offsets for each topic, or None if the default
            behavior should be employed instead. The default behavior is
            picking up the last committed offsets of topics.
            This method must be implemented if topic state is to be stored
            in some system other than Kafka, for example when writing data from
            Kafka into a transactional store.
        pre_topic_refresh_callback: (Optional[Callable[[list[str], list[str]],
            Any]]): Optional callback that gets executed right before the
            consumer is about to refresh the topics. The callback function is
            passed in a list of topic names Consumer is currently consuming
            from (old topics) and a list of topic names Consumer will be
            consuming from (old topics and new topics). The return value of the
            function is ignored.

    Note:
        The Consumer leverages the yelp_kafka `KafkaConsumerGroup`.

    **Examples**:

    A simple example can be a consumer with name 'my_consumer' that
    consumes a message from multiple topics, processes it and
    commits the offset and this process continues::

        with Consumer(
            consumer_name='my_consumer',
            team_name='bam',
            expected_frequency_seconds=12345,
            topic_to_consumer_topic_state_map={
                'topic_a': None,
                'topic_b': None
            }
        ) as consumer:
            while True:
                message = consumer.get_message()
                if message is not None:
                    ... do stuff with message ...
                    consumer.commit_message(message)

    Note:
        Recommended to avoid calling `commit_message(message)` after every
        message, as it is relatively expensive.

    Another example can be a consumer which consumes multiple messages
    (with maximum number of messages in batch as `count`) from 2 topics
    'topic_a' and 'topic_b', processes them and commits them::

        with Consumer(
            consumer_name='my_consumer',
            team_name='bam',
            expected_frequency_seconds=12345,
            topic_to_consumer_topic_state_map={
                'topic_a': None,
                'topic_b': None
            }
        ) as consumer:
            while True:
                messages = consumer.get_messages(
                    count=batch_size,
                    blocking=True,
                    timeout=batch_timeout
                )
                if messages:
                    ... do stuff with messages ...
                    consumer.commit_messages(messages)

    Note:
        It's recommended to retrieve messages in batches via
        `get_messages(..)`, do your work with them, and then commit them as
        a group with a single call to `commit_messages(..)`
    """

    def _start(self):
        self.consumer_group = KafkaConsumerGroup(
            topics=self.topic_to_partition_map.keys(),
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
        """ Retrieve a list of messages from the message buffer, optionally
        blocking until the requested number of messages has been retrieved.

        Warning:
            If `blocking` is True and `timeout` is None this will block until
            the requested number of messages is retrieved, potentially blocking
            forever. Please be absolutely sure this is what you are intending
            if you use these options!

        Args:
            count (int): Number of messages to retrieve
            blocking (boolean): Set to True to block while waiting for messages
                if the buffer has been depleted. Otherwise returns immediately
                if the buffer reaches depletion. Default is False.
            timeout (double): Maximum time (in seconds) to wait if blocking is
                set to True. Set to None to wait indefinitely.

        Returns:
            ([data_pipeline.message.Message]): List of Message objects with
            maximum size `count`, but may be smaller or empty depending on
            how many messages were retrieved within the timeout.
        """
        # TODO(tajinder|DATAPIPE-1231): Consumer should refresh topics
        # periodically even if NO timeout is provided and there are no
        # messages to consume.
        messages = []
        has_timeout = timeout is not None
        if has_timeout:
            max_time = time() + timeout
        while len(messages) < count:
            # Consumer refreshes the topics periodically only if consumer_source
            # is specified and would use the `fetch_offsets_for_topics` callback
            # to get the partition offsets corresponding to the topics.
            if self.consumer_source:
                self._refresh_source_topics_if_necessary()
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
                message,
                self.force_payload_decode,
                reader_schema_id=self._topic_to_reader_schema_map.get(message.topic)
            )
            messages.append(message)
            # Update state in registrar for Producer/Consumer registration in milliseconds
            self.registrar.update_schema_last_used_timestamp(message.schema_id, long(1000 * time()))

            if not blocking or (has_timeout and time() > max_time):
                break
        return messages

    def _refresh_source_topics_if_necessary(self):
        # TODO(tajinder|DATAPIPE-1265): Consumer after refreshing topics should
        # only tail new topics.
        if not self._refresh_timer.should_tick():
            return

        current_topics = self.topic_to_partition_map.keys()

        refreshed_topic_to_state_map = self._get_topic_to_offset_map(
            self.topic_to_partition_map,
            self.consumer_source
        )
        refreshed_topics = refreshed_topic_to_state_map.keys()

        if set(current_topics) == set(refreshed_topics):
            return

        if self.pre_topic_refresh_callback:
            self.pre_topic_refresh_callback(current_topics, refreshed_topics)
        self.reset_topics(refreshed_topic_to_state_map)
