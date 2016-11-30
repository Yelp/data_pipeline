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

from collections import defaultdict
from contextlib import contextmanager

from cached_property import cached_property
from kafka import KafkaClient
from kafka.common import FailedPayloadsError
from kafka.common import OffsetCommitRequest
from kafka.util import kafka_bytestring
from yelp_kafka.config import KafkaConsumerConfig

from data_pipeline._consumer_tick import _ConsumerTick
from data_pipeline._retry_util import ExpBackoffPolicy
from data_pipeline._retry_util import retry_on_exception
from data_pipeline._retry_util import RetryPolicy
from data_pipeline.client import Client
from data_pipeline.config import get_config
from data_pipeline.consumer_source import FixedSchemas
from data_pipeline.envelope import Envelope
from data_pipeline.message import Message
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
# from yelp_kafka import discovery

logger = get_config().logger


class MultipleClusterTypeError(Exception):
    def __init__(self, *cluster_types):
        err_message = (
            "Consumer can not process topics from different kafka cluster "
            "types, i.e. (" + ", ".join(cluster_types) + ")."
        )
        Exception.__init__(self, err_message)


class TopicNotFoundInRegionError(Exception):
    def __init__(self, topic_name, cluster_type, cluster_name):
        err_message = (
            "Topic `{}` not found in `{}` kafka type cluster in `{}` "
            "region".format(topic_name, cluster_type, cluster_name)
        )
        Exception.__init__(self, err_message)


class ConsumerTopicState(object):
    """
    ConsumerTopicState object holds the state of a consumer topic with
    partition_offset_map mapping all the partitions of the topic with their
    last seen offsets and last_seen_schema_id with the schema id of the
    latest processed message.
    Whenever consumer receives a yelp_kafka message for a topic, it builds a
    data_pipeline.message.Message and updates the topic state map for the
    topic with updated ConsumerTopicState (updates last_seen_schema_id with
    the schema_id of the message and partition offset with the offset of
    the message).

    Args:
        partition_offset_map ({int:int}): map of partitions to their last
            seen offsets.
        last_seen_schema_id: The last seen schema_id.
    """

    def __init__(self, partition_offset_map, last_seen_schema_id):
        self.partition_offset_map = partition_offset_map
        self.last_seen_schema_id = last_seen_schema_id

    def __repr__(self):
        return '<ConsumerTopicState(last_seen_schema_id: {0}, partition_offset_map: {1})>'.format(
            self.last_seen_schema_id,
            self.partition_offset_map
        )


class BaseConsumer(Client):
    """
    This is the base interface for the Consumer implementations. It provides
    a set of methods common to the derived consumer classes and specifies
    which methods should be overridden. It should not be instantiated and
    has only been made public to ensure docs are generated.

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
        use_group_sha (Optional[boolean]): Used by partitioner to establish
            group membership. If false, consumer group with same name will
            be treated as the same group; otherwise, they will be different
            since group sha is different. Default is true.
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
        pre_topic_refresh_callback: (Optional[Callable[[set[str], set[str]],
            Any]]): Optional callback that gets executed right before the
            consumer is about to refresh the topics. The callback function is
            passed in a set of topic names Consumer is currently consuming
            from (current_topics) and a set of topic names Consumer will be
            consuming from (refreshed_topics). The return value of the
            function is ignored.
        cluster_name: (Optional[string]): Indicates the name (region) of kafka
            cluster the topics belong to. Ex: uswest2-prod. Accordingly the
            Consumer will connect to Kafka cluster in the corresponding region.
            All topics should belong to the same kafka cluster name.
            Defaults to None.
    """

    def __init__(
        self,
        consumer_name,
        team_name,
        expected_frequency_seconds,
        topic_to_consumer_topic_state_map=None,
        consumer_source=None,
        force_payload_decode=True,
        auto_offset_reset='smallest',
        partitioner_cooldown=get_config().consumer_partitioner_cooldown_default,
        use_group_sha=get_config().consumer_use_group_sha_default,
        topic_refresh_frequency_seconds=get_config().topic_refresh_frequency_seconds,
        pre_rebalance_callback=None,
        post_rebalance_callback=None,
        fetch_offsets_for_topics=None,
        pre_topic_refresh_callback=None,
        cluster_name=None
    ):
        super(BaseConsumer, self).__init__(
            consumer_name,
            team_name,
            expected_frequency_seconds,
            monitoring_enabled=False
        )

        if ((topic_to_consumer_topic_state_map and consumer_source) or
                (not topic_to_consumer_topic_state_map and not consumer_source)):
            raise ValueError("Exactly one of topic_to_consumer_topic_state_map "
                             "or consumer_source must be specified")

        self.consumer_source = consumer_source
        self.topic_to_consumer_topic_state_map = topic_to_consumer_topic_state_map
        self.force_payload_decode = force_payload_decode
        self.auto_offset_reset = auto_offset_reset
        self.partitioner_cooldown = partitioner_cooldown
        self.use_group_sha = use_group_sha
        self.running = False
        self.consumer_group = None
        self.pre_rebalance_callback = pre_rebalance_callback
        self.post_rebalance_callback = post_rebalance_callback
        self.fetch_offsets_for_topics = fetch_offsets_for_topics
        self.pre_topic_refresh_callback = pre_topic_refresh_callback
        self.cluster_name = self._set_cluster_name(cluster_name)
        self._refresh_timer = _ConsumerTick(
            refresh_time_seconds=topic_refresh_frequency_seconds
        )
        self._topic_to_reader_schema_map = self._get_topic_to_reader_schema_map(
            consumer_source
        )
        self._consumer_retry_policy = RetryPolicy(
            ExpBackoffPolicy(with_jitter=True),
            max_retry_count=get_config().consumer_max_offset_retry_count
        )
        self._envelope = Envelope()
        if self.topic_to_consumer_topic_state_map:
            self.cluster_type = self._determine_cluster_type_from_topics(
                self.topic_to_consumer_topic_state_map.keys()
            )

    @cached_property
    def _schematizer(self):
        return get_schematizer()

    def _set_cluster_name(self, cluster_name):
        return cluster_name or get_config().kafka_cluster_name

    def _determine_cluster_type_from_topics(self, topic_names):
        """Checks whether the cluster type of all topics are the same and
        returns it.

        :param topic_names: list of topic names
        :raises MultipleClusterTypeError: if all topics don't have the same cluster type.
        :return: cluster_type
        """
        cluster_type = None
        for topic_name in topic_names:
            topic = self._schematizer.get_topic_by_name(topic_name)
            if cluster_type is None:
                cluster_type = topic.cluster_type
            elif cluster_type != topic.cluster_type:
                raise MultipleClusterTypeError(
                    cluster_type,
                    topic.cluster_type
                )
        return cluster_type

    def _get_refreshed_topic_to_consumer_topic_state_map(
        self,
        topic_to_consumer_state_map,
        consumer_source
    ):
        if topic_to_consumer_state_map:
            return topic_to_consumer_state_map
        if consumer_source:
            return self._get_topic_to_offset_map(consumer_source.get_topics())
        return {}

    def _get_topic_to_offset_map(self, topics):
        """ This function constructs a new topic_to_consumer_topic_state_map
        from the given topics set. Return the topic_to_consumer_topic_state_map
        dictionary.
        """
        if self.fetch_offsets_for_topics:
            topic_to_partition_offsets_map = self.fetch_offsets_for_topics(
                list(topics)
            )
            return {
                topic: (
                    ConsumerTopicState(
                        partition_offset_map,
                        last_seen_schema_id=None
                    ) if partition_offset_map else None
                )
                for topic, partition_offset_map in
                topic_to_partition_offsets_map.iteritems()
            }
        else:
            return {topic: None for topic in topics}

    def _get_topic_to_reader_schema_map(self, consumer_source):
        """
        In case of FixedSchema consumer source, creates a dictionary with topic
        name as key and reader schema id as value that would be used to decode
        the messages of that topic. Return the topic_to_reader_schema_map
        dictionary.
        """
        topic_to_reader_schema_map = {}
        if isinstance(consumer_source, FixedSchemas):
            schema_to_topic_map = consumer_source.get_schema_to_topic_map()
            for schema_id, topic_name in schema_to_topic_map.iteritems():
                if topic_name not in topic_to_reader_schema_map:
                    topic_to_reader_schema_map[topic_name] = schema_id
                else:
                    raise ValueError(
                        'Multiple reader schemas ({schema1}, {schema2}) exist '
                        'for topic: {topic}'.format(
                            schema1=topic_to_reader_schema_map[topic_name],
                            schema2=schema_id,
                            topic=topic_name
                        )
                    )
        return topic_to_reader_schema_map

    def _set_topic_to_partition_map(self, topic_to_consumer_topic_state_map):
        """ This function takes a topic_to_consumer_topic_state_map and sets
        the topic_to_partition_map instance variable with topic names as keys
        and corresponding partition lists as values.
        """
        self.cluster_type = self._determine_cluster_type_from_topics(
            topic_to_consumer_topic_state_map.keys()
        )
        self.topic_to_partition_map = {}
        for (
            topic_name, consumer_topic_state
        ) in topic_to_consumer_topic_state_map.iteritems():
            topics = self._get_topics_in_region_from_topic_name(topic_name)
            for topic in topics:
                self.topic_to_partition_map[topic] = (
                    consumer_topic_state.partition_offset_map.keys()
                    if consumer_topic_state else None
                )
        self._set_registrar_tracked_schema_ids(topic_to_consumer_topic_state_map)

    def _get_topics_in_region_from_topic_name(self, topic_name):
        """We use yelp_kafka discovery to get topic for a specific
        cluster_type and cluster_name. There should 0 or 1 topic for a given
        name in a particular cluster_type and cluster_name. However the
        discovery methods returns a list of topics. Hence keeping yelp_kafka
        as the source of truth for topic info, we will tail all topics that it
        returns, which almost always is 0 or 1.
        """
        if self.cluster_type == 'scribe':
            return self._get_scribe_topics_from_topic_name(topic_name)
        else:
            return self._get_kafka_topics_from_topic_name(topic_name)

    def _get_scribe_topics_from_topic_name(self, topic_name):
        """yelp_kafka.discovery.get_region_logs_stream returns a list of
        ([topics], cluster) tuple. However if the region is specified we should
        have 0 or 1 tuple of ([topics], cluster) returned
        http://servicedocs.yelpcorp.com/docs/yelp_kafka/discovery.html#yelp_kafka.discovery.get_region_logs_stream
        """
        # TODO [askatti#DATAPIPE-2137|2016-11-28] Use discovery methods after
        # adding kafkadiscovery container to make tests work
        # topics_in_region = discovery.get_region_logs_stream(
        #     client_id=self.client_name,
        #     stream=topic_name,
        #     region=self.cluster_name
        # )
        # if not topics_in_region:
        #     raise TopicNotFoundInRegionError(
        #         topic_name,
        #         self.cluster_type,
        #         self.cluster_name
        #     )
        # topics, _ = topics_in_region[0]
        # return topics
        return [topic_name]

    def _get_kafka_topics_from_topic_name(self, topic_name):
        """yelp_kafka.discovery.search_topic returns a list of (topic, cluster)
        tuple. However if the region is specified we should have 0 or 1 tuple
        of (topic, cluster) returned
        http://servicedocs.yelpcorp.com/docs/yelp_kafka/discovery.html#yelp_kafka.discovery.search_topic
        """
        # TODO [askatti#DATAPIPE-2137|2016-11-28] Use discovery methods after
        # adding kafkadiscovery container to make tests work
        # topics_in_region = discovery.search_topic(
        #     topic=topic_name,
        #     clusters=[self._region_cluster_config]
        # )
        # if not topics_in_region:
        #     raise TopicNotFoundInRegionError(
        #         topic_name,
        #         self.cluster_type,
        #         self._region_cluster_config.name
        #     )
        # return [topic for topic, _ in topics_in_region]
        return [topic_name]

    def _set_registrar_tracked_schema_ids(self, topic_to_consumer_topic_state_map):
        """
        Register what schema ids to track based on the topic map passed in
        on initialization and during topic change.
        """
        schema_id_list = [
            consumer_topic_state.last_seen_schema_id
            for consumer_topic_state in topic_to_consumer_topic_state_map.itervalues()
            if consumer_topic_state and consumer_topic_state.last_seen_schema_id
        ]
        self.registrar.register_tracked_schema_ids(schema_id_list)

    @cached_property
    def kafka_client(self):
        """ Returns the `KafkaClient` object."""
        return KafkaClient(self._region_cluster_config.broker_list)

    @property
    def client_type(self):
        """ Returns a string identifying the client type. """
        return "consumer"

    def __enter__(self):
        self.start()
        self.registrar.start()
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
        self.reset_topic_to_partition_offset_cache()
        refreshed_topic_to_consumer_topic_state_map = (
            self._get_refreshed_topic_to_consumer_topic_state_map(
                self.topic_to_consumer_topic_state_map,
                self.consumer_source
            ))
        if refreshed_topic_to_consumer_topic_state_map:
            self._commit_topic_offsets(
                refreshed_topic_to_consumer_topic_state_map
            )
            # After committing partition offsets, the
            # self.topic_to_consumer_topic_state_map is set to `None` to
            # prevent committing these offsets again.
            self.topic_to_consumer_topic_state_map = None
            self._set_topic_to_partition_map(
                refreshed_topic_to_consumer_topic_state_map
            )
        self._start_consumer()

    def _start_consumer(self):
        logger.info("Starting Consumer '{0}'...".format(self.client_name))
        if self.running:
            raise RuntimeError("Consumer '{0}' is already running".format(
                self.client_name
            ))
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
        self.registrar.stop()
        self.kafka_client.close()
        self.reset_topic_to_partition_offset_cache()
        self.running = False
        logger.info("Consumer '{0}' stopped".format(self.client_name))

    def __iter__(self):
        while True:
            yield self.get_message(
                blocking=True,
                timeout=get_config().consumer_get_messages_timeout_default
            )

    def reset_topic_to_partition_offset_cache(self):
        self.topic_to_partition_offset_map_cache = defaultdict(dict)

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
            partition_offset_map = topic_to_partition_offset_map.get(
                message.topic,
                {}
            )
            max_offset = partition_offset_map.get(pos_info.partition, 0)
            # Increment the offset value by 1 so the consumer knows where to
            # retrieve the next message.
            partition_offset_map[pos_info.partition] = (
                max(pos_info.offset, max_offset) + 1
            )
            topic_to_partition_offset_map[message.topic] = partition_offset_map
        self.commit_offsets(topic_to_partition_offset_map)

    def commit_offsets(self, topic_to_partition_offset_map):
        """Commits offset information to kafka.  Allows lower-level control for
        committing offsets.  In general, :meth:`commit_message` or
        :meth:`commit_messages` should be used, but this can be useful when paired with
        :meth:`data_pipeline.position_data.PositionData.topic_to_last_position_info_map`.

        **Example**::
            The `topic_to_partition_offset_map` should be formatted like::

                {
                  'topic1': {0: 83854, 1: 8943892},
                  'topic2': {0: 190898}
                }

        Args::
            topic_to_partition_offset_map (Dict[str, Dict[int, int]]): Maps from
                topics to a partition and offset map for each topic.
        """
        topic_to_partition_offset_map = self._get_offsets_map_to_be_committed(
            topic_to_partition_offset_map
        )
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

    def _get_offsets_map_to_be_committed(
        self,
        topic_to_partition_offset_map
    ):
        filtered_topic_to_partition_offset_map = defaultdict(dict)
        for topic, partition_map in topic_to_partition_offset_map.items():
            for partition, offset in partition_map.items():
                if (partition not in self.topic_to_partition_offset_map_cache[topic] or
                        self.topic_to_partition_offset_map_cache[topic][partition] != offset):
                    self.topic_to_partition_offset_map_cache[topic][partition] = offset
                    filtered_topic_to_partition_offset_map[topic][partition] = offset
        return filtered_topic_to_partition_offset_map

    @contextmanager
    def ensure_committed(self, messages):
        """ Context manager which calls `commit_messages` on exit, useful for
        ensuring a group of messages are committed even if an error is
        encountered.

        **Example**::

            for msg in consumer:
                if msg is None:
                    continue
                with consumer.ensure_committed(msg):
                    do_things_with_message(msg)

        **Example**::

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

        **Example**::

            with Consumer(
                consumer_name='example',
                team_name='bam',
                expected_frequency_seconds=12345,
                topic_to_consumer_topic_state_map={'topic1': None}
            ) as consumer:
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
                    topic_map = {}
                    for topic in consumer.topic_to_partition_map.keys():
                        topic_map[topic] = None
                    new_topics = get_new_topics()
                    for topic in new_topics:
                        if topic not in topic_map:
                            topic_map[topic] = None
                    consumer.reset_topics(topic_map)

        Note:
            This is an expensive operation, roughly equivalent to destroying
            and recreating the Consumer, so make sure you only are calling this
            when absolutely necessary.

            It's also important to note that you should probably be calling
            `commit_messages` just prior to calling this, otherwise the offsets
            may be lost for the topics you were previously tailing.

        Args:
            topic_to_consumer_topic_state_map ({str:Optional(ConsumerTopicState)}):
                A map of topic names to `ConsumerTopicState` objects which
                define the offsets to start from. The ConsumerTopicState of a topic
                may be `None`, in which case the committed kafka offset for the
                consumer_name is used. If there is no committed kafka offset
                for the consumer_name the Consumer will begin from the
                `auto_offset_reset` offset in the topic.
        """
        self.stop()
        self._commit_topic_offsets(topic_to_consumer_topic_state_map)
        self._set_topic_to_partition_map(topic_to_consumer_topic_state_map)
        self._start_consumer()

    def _commit_topic_offsets(self, topic_to_consumer_topic_state_map):
        if topic_to_consumer_topic_state_map:
            logger.info("Committing offsets for Consumer '{0}'...".format(
                self.client_name
            ))
            topic_to_partition_offset_map = {}
            for topic, consumer_topic_state in topic_to_consumer_topic_state_map.iteritems():
                if consumer_topic_state is None:
                    continue
                topic_to_partition_offset_map[topic] = consumer_topic_state.partition_offset_map
            self.commit_offsets(topic_to_partition_offset_map)
            logger.info("Offsets committed for Consumer '{0}'...".format(
                self.client_name
            ))

    def _send_offset_commit_requests(self, offset_commit_request_list):
        if len(offset_commit_request_list) > 0:
            retry_on_exception(
                self._consumer_retry_policy,
                (FailedPayloadsError),
                self.kafka_client.send_offset_commit_request,
                group=kafka_bytestring(self.client_name),
                payloads=offset_commit_request_list
            )

    @property
    def _region_cluster_config(self):
        """ The ClusterConfig for Kafka cluster to connect to. If cluster_name
        is not specified, it will default to the value set in Config"""
        # TODO [askatti#DATAPIPE-2137|2016-11-28] Use discovery methods after
        # adding kafkadiscovery container to make tests work
        # if self.cluster_name:
        #     return discovery.get_kafka_cluster(
        #         cluster_type=self.cluster_type,
        #         client_id=self.client_name,
        #         cluster_name=self.cluster_name
        #     )
        # else:
        #     return get_config().cluster_config
        return get_config().cluster_config

    @property
    def _kafka_consumer_config(self):
        """ The `KafkaConsumerConfig` for the Consumer.

        Notes:
            This is not a `@cached_property` since there is the possibility
            that the cluster_config could change during runtime and users could
            leverage this for responding to topology changes.

            `auto_commit` is set to False to ensure clients can determine when
            they want their topic offsets committed via commit_messages(..)
        """
        return KafkaConsumerConfig(
            group_id=self.client_name,
            cluster=self._region_cluster_config,
            auto_offset_reset=self.auto_offset_reset,
            auto_commit=False,
            partitioner_cooldown=self.partitioner_cooldown,
            use_group_sha=self.use_group_sha,
            pre_rebalance_callback=self.pre_rebalance_callback,
            post_rebalance_callback=self._apply_post_rebalance_callback_to_partition
        )

    def _apply_post_rebalance_callback_to_partition(self, partitions):
        """
        Removes the topics not present in the partitions list
        from the consumer topic_to_partition_map

        Args:
            partitions: List of current partitions the kafka
            broker holds
        """
        self.topic_to_partition_map = dict(partitions)
        self.reset_topic_to_partition_offset_cache()

        if self.post_rebalance_callback:
            return self.post_rebalance_callback(partitions)

    def refresh_new_topics(
        self,
        topic_filter=None,
        pre_topic_refresh_callback=None
    ):
        """
        Get newly created topics that match given criteria and currently not
        being tailed by the consumer and refresh internal topics list for these
        new topics.

        **Example**::

            topic_filter = TopicFilter(namespace_name='test_namespace')
            with Consumer(
                consumer_name='example',
                team_name='bam',
                expected_frequency_seconds=12345,
                topic_to_consumer_topic_state_map={'topic1': None, 'topic2': None}
            ) as consumer:
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
                    consumer.refresh_new_topics(
                        topic_filter,
                        pre_topic_refresh_callback
                    )

        Note:
            pre_topic_refresh_callback can be used to perform custom logic and
            takes the currently tailed topics and the newly added topics as
            arguments. For example, in the aforementioned example, if a new
            topic 'topic3' is being added to namespace 'test_namespace' then
            `pre_topic_refresh_callback(['topic1', 'topic2'], ['topic3'])`
            would be executed.

            Commit the offsets of already consumed messages before refreshing
            to prevent duplicate messages. Else, the consumer will pick up the
            last committed offsets of topics and will consume already consumed
            messages.

        Args:
            topic_filter (Optional[TopicFilter]): criteria to filter newly
                created topics.
            pre_topic_refresh_callback:
                (Optional[Callable[[list[str], list[str]], Any]]): function
                that performs custom logic before the consumer start tailing
                new topics. The function will take a list of old_topic_names
                and list of new_topic_names. The return value of the function
                is ignored.

        Returns:
            [data_pipeline.schematizer_clientlib.models.Topic]: A list of new topics.
        """
        # TODO [DATAPIPE-843|clin] deprecate this function in favor of new
        # `refresh_topics` function once AST is revised to use the new function.
        new_topics = self._get_new_topics(topic_filter)
        new_topics = [topic for topic in new_topics
                      if topic.name not in self.topic_to_partition_map]

        if new_topics:
            new_topic_names = [topic.name for topic in new_topics]
            old_topic_names = self.topic_to_partition_map.keys()
            if pre_topic_refresh_callback:
                pre_topic_refresh_callback(old_topic_names, new_topic_names)
            self.stop()
            self._update_topic_to_partition_map(new_topic_names)
            self._start_consumer()

        return new_topics

    def _get_new_topics(self, topic_filter):
        new_topics = self._schematizer.get_topics_by_criteria(
            namespace_name=topic_filter.namespace_name,
            source_name=topic_filter.source_name,
            created_after=topic_filter.created_after
        )
        if topic_filter.filter_func:
            new_topics = topic_filter.filter_func(new_topics)
        return new_topics

    def _update_topic_to_partition_map(self, new_topics):
        for new_topic in new_topics:
            self.topic_to_partition_map[new_topic] = None

    def refresh_topics(self, consumer_source):
        """Refresh the topics this consumer is consuming from based on the
        settings in the given consumer_source.

        Args:
            consumer_source (data_pipeline.consumer_source.ConsumerSource): one
            of ConsumerSource types, such as SingleTopic, TopicsInFixedNamespaces.

        Returns:
            List[str]: A list of new topic names that this consumer starts to
            consume.
        """
        topics = consumer_source.get_topics()
        new_topics = [topic for topic in topics
                      if topic not in self.topic_to_partition_map]

        if new_topics:
            self.stop()
            self._update_topic_to_partition_map(new_topics)
            self._start_consumer()

            # If a new topic doesn't exist, when the consumer restarts, it will
            # be removed from the topic state map after the re-balance callback.
            # In such case, the non-existent topics are removed from the new_topics.
            new_topics = [topic for topic in new_topics
                          if topic in self.topic_to_partition_map]

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
