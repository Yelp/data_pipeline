# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

from cached_property import cached_property
from kafka import KafkaClient
from kafka.common import OffsetCommitRequest
from kafka.util import kafka_bytestring
from yelp_kafka.config import KafkaConsumerConfig

from data_pipeline.client import Client
from data_pipeline.config import get_config
from data_pipeline.message import Message
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


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


class BaseConsumer(Client):
    """This is the base interface for the MultiprocessingConsumer and Consumer
    implementations. It provides a set of methods common to the derived consumer
    classes and specifies which methods should be overriden. It should not be
    instantiated and has only been made public to ensure docs are generated."""

    def __init__(
        self,
        consumer_name,
        team_name,
        expected_frequency_seconds,
        topic_to_consumer_topic_state_map,
        force_payload_decode=True,
        auto_offset_reset='smallest',
        partitioner_cooldown=get_config().consumer_partitioner_cooldown_default,
        pre_rebalance_callback=None,
        post_rebalance_callback=None
    ):
        """ Creates the base BaseConsumer object

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
            auto_offset_reset (str): Used for offset validation. If 'largest'
                reset the offset to the latest available message (tail). If
                'smallest' reset from the earliest (head).
            partitioner_cooldown (float): Waiting time (in seconds) for the
                consumer to acquire the partitions. See
                yelp_kafka/yelp_kafka/partitioner.py for more details
            pre_rebalance_callback: Optional callback called prior to
                topic/partition rebalancing.
                Required args, dict of partitions to offset.
            post_rebalance_callback: Optional callback called following
                topic/partition rebalancing.
                Required args, dict of partitions to offset.
        """
        super(BaseConsumer, self).__init__(
            consumer_name,
            team_name,
            expected_frequency_seconds,
            monitoring_enabled=False
        )
        self.topic_to_consumer_topic_state_map = topic_to_consumer_topic_state_map
        self.force_payload_decode = force_payload_decode
        self.auto_offset_reset = auto_offset_reset
        self.partitioner_cooldown = partitioner_cooldown
        self.running = False
        self.consumer_group = None
        self.pre_rebalance_callback = pre_rebalance_callback
        self.post_rebalance_callback = post_rebalance_callback

    @cached_property
    def kafka_client(self):
        return KafkaClient(get_config().cluster_config.broker_list)

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
            The derived class must implement _start().
        """
        logger.info("Starting Consumer '{0}'...".format(self.client_name))
        if self.running:
            raise RuntimeError("Consumer '{0}' is already running".format(
                self.client_name
            ))
        self._commit_topic_map_offsets()
        self._start()
        self.running = True
        logger.info("Consumer '{0}' started".format(self.client_name))

    def stop(self):
        """ Stop the Consumer. Normally this should NOT be called directly,
        rather the Consumer should be used as a context manager, which will
        stop automatically when the context exits.

        Note:
            The derived class must implement _stop().
        """
        logger.info("Stopping Consumer '{0}'...".format(self.client_name))
        if self.running:
            self._stop()
        self.kafka_client.close()
        self.running = False
        logger.info("Consumer '{0}' stopped".format(self.client_name))

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

    def get_messages(
        self,
        count,
        blocking=False,
        timeout=get_config().consumer_get_messages_timeout_default
    ):
        """ Retrieve a list of messages from the message buffer, optionally
        blocking until the requested number of messages has been retrieved.

        Note:
            The derived class must implement this method.

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
            # Increment the offset value by 1 so the consumer knows where to retrieve the next message.
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
                        topic=kafka_bytestring(topic),
                        partition=partition,
                        offset=offset,
                        metadata=None
                    )
                )
        self._send_offset_commit_requests(offset_requests)

    def _send_offset_commit_requests(self, offset_commit_request_list):
        if len(offset_commit_request_list) > 0:
            self.kafka_client.send_offset_commit_request(
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
            partitioner_cooldown=self.partitioner_cooldown,
            pre_rebalance_callback=self.pre_rebalance_callback,
            post_rebalance_callback=self.apply_post_rebalance_callback_to_partition
        )

    def apply_post_rebalance_callback_to_partition(self, partitions):
        """
        Removes the topics not present in the partitions list
        from the topic_to_consumer_topic_state_map

        Args:
            partitions: List of current partitions the kafka
            broker holds
        """
        self.topic_to_consumer_topic_state_map = {
            key: self.topic_to_consumer_topic_state_map.get(key)
            for key in partitions
        }

        if self.post_rebalance_callback:
            return self.post_rebalance_callback(partitions)

    def refresh_new_topics(
        self,
        topic_filter=None,
        before_refresh_handler=None
    ):
        """
        Get newly created topics that match given criteria and refresh internal
        topic state maps for these new topics.

        Args:
            topic_filter (Optional[TopicFilter]): criteria to filter newly
                created topics.
            before_refresh_handler
                (Optional[Callable[[List[data_pipeline.schematizer_clientlib.models.Topic]], Any]]):
                function that performs custom logic before the consumer resets
                topics.  The function will take a list of new topics filtered
                by the given filter and currently not in the topic state map.
                The return value of the function is ignored.

        Returns:
            [data_pipeline.schematizer_clientlib.models.Topic]: A list of new topics.
        """
        # TODO [DATAPIPE-843|clin] deprecate this function in favor of new
        # `refresh_topics` function once AST is revised to use the new function.
        new_topics = self._get_new_topics(topic_filter)

        new_topics = [topic for topic in new_topics
                      if topic.name not in self.topic_to_consumer_topic_state_map]

        if before_refresh_handler:
            before_refresh_handler(new_topics)

        if new_topics:
            self.stop()
            self._update_new_topic_state_map(new_topics)
            self.start()

        return new_topics

    def _get_new_topics(self, topic_filter):
        new_topics = get_schematizer().get_topics_by_criteria(
            namespace_name=topic_filter.namespace_name,
            source_name=topic_filter.source_name,
            created_after=topic_filter.created_after
        )
        if topic_filter.filter_func:
            new_topics = topic_filter.filter_func(new_topics)
        return new_topics

    def _update_new_topic_state_map(self, new_topics):
        for new_topic in new_topics:
            self.topic_to_consumer_topic_state_map[new_topic.name] = None

    def refresh_topics(self, consumer_source):
        """Refresh the topics this consumer are consuming from based on the
        settings in the given consumer_source.

        Args:
            consumer_source (data_pipeline.consumer_source.ConsumerSource): one
                of ConsumerSource types, such as SingleTopic, TopicInNamespace.

        Returns:
            List[str]: A list of new topic names that this consumer starts to
            consume.
        """
        topics = consumer_source.get_topics()
        new_topics = [topic for topic in topics
                      if topic not in self.topic_to_consumer_topic_state_map]

        if new_topics:
            self.stop()
            for new_topic in new_topics:
                self.topic_to_consumer_topic_state_map[new_topic] = None
            self.start()

            # If a new topic doesn't exist, when the consumer restarts, it will
            # be removed from the topic state map after the re-balance callback.
            # In such case, the non-existent topics are removed from the new_topics.
            new_topics = [topic for topic in new_topics
                          if topic in self.topic_to_consumer_topic_state_map]

        return new_topics


class TopicFilter(object):
    """Criteria to filter topics.

    Args:
        namespace_name (Optional[str]): filter topics by their namespace name.
        source_name (Optional[str]): filter topics by their source name. Note
            that same source name may appear in multiple namespaces.
        created_after (Optional[int]): get topics created after this timestamp.
            The topics created at the same timestamp are included as well.
        filter_func (Optional[function]): function that performs custom logic
            to filter topics.  The input of this function will be a list of
            topics already filtered by specified namespace, source, and/or
            created_after timestamp.  This function should return a list of
            filtered topics.
    """

    def __init__(
        self,
        namespace_name=None,
        source_name=None,
        created_after=None,
        filter_func=None
    ):
        self.namespace_name = namespace_name
        self.source_name = source_name
        self.created_after = created_after
        self.filter_func = filter_func
