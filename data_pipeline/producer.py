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

import copy
import multiprocessing
import time
from collections import defaultdict

import simplejson as json
from cached_property import cached_property

from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline._kafka_util import get_actual_published_messages_count
from data_pipeline._pooled_kafka_producer import PooledKafkaProducer
from data_pipeline.client import Client
from data_pipeline.config import get_config


logger = get_config().logger


class PublicationUnensurableError(Exception):
    pass


class Producer(Client):
    """Producers are context managers, that provide a high level interface for
    publishing :class:`data_pipeline.message.Message` instances into the data
    pipeline.

    When messages are handed to a producer via the :meth:`publish` method, they
    aren't immediately published into Kafka.  Instead, they're buffered until
    a number of messages are accumulated, or too much time has passed,
    then published all at once.  This process is designed to be largely
    transparent to the user.

    .. note::

        The clientlib used to include an AsyncProducer, which published to Kafka in the
        background.  This producer was somewhat flaky, increased development effort,
        and didn't provide a concrete performance benefit (see
        pb/150070 for benchmark results).  If we ever want to
        revive that producer, a SHA containing the producer just before its removal
        has been tagged as before-async-producer-removal.

    **Examples**:

      At it's simplest, start a producer and publish messages to it::

          with Producer() as producer:
              producer.publish(message)

      Messages are not immediately published, but are buffered.  Consequently,
      it may sometimes be necessary to flush the buffer before doing some
      tasks::

          with Producer() as producer:
              while upstream.has_another_batch_of_messages():
                  for message in upstream.get_messages_from_upstream():
                      producer.publish()

                  producer.flush()
                  upstream.all_those_messages_were published()

      The Producer is incapable of flushing its own buffers, which can be
      problematic if messages aren't published relatively constantly.  The
      :meth:`wake` should be called periodically to allow the Producer to clear
      its buffer if necessary, in the absence of messages.  If :meth:`wake`
      isn't called, messages in the buffer could be delayed indefinitely::

          with Producer() as producer:
              while True:
                  try:
                      message = slow_queue.get(block=True, timeout=0.1)
                      producer.publish(message)
                  except Empty:
                      producer.wake()

    Args:
      producer_name (str): See parameter `client_name` in
        :class:`data_pipeline.client.Client`.
      team_name (str): See parameter `team_name` in
        :class:`data_pipeline.client.Client`.
      expected_frequency_seconds (str): See parameter
        `expected_frequency_seconds` in :class:`data_pipeline.client.Client`.
      use_work_pool (bool): If true, the process will use a multiprocessing
        pool to serialize messages in preparation for transport.  The work pool
        can parallelize some expensive serialization.  Default is false.
      position_data_callback (Optional[function]): If provided, the function
        will be called when the producer starts, and whenever messages are
        committed to Kafka, with updated position data.  The callback should
        take a single argument, which will be an instance of
        :class:`PositionData`.
      dry_run (Optional[bool]): If true, producer will skip publishing message
        to kafka. Default is false.
      monitoring_enabled (Optional[bool]): If true, monitoring will be enabled
        to record client's activities. Default is true.
    """

    def __init__(
        self,
        producer_name,
        team_name,
        expected_frequency_seconds,
        use_work_pool=False,
        dry_run=False,
        position_data_callback=None,
        monitoring_enabled=True,
        schema_id_list=None
    ):
        super(Producer, self).__init__(
            producer_name,
            team_name,
            expected_frequency_seconds,
            monitoring_enabled,
            dry_run=dry_run
        )
        self.use_work_pool = use_work_pool
        self.dry_run = dry_run
        self.position_data_callback = position_data_callback
        if schema_id_list is None:
            schema_id_list = []
        # Send initial producer registration messages
        self.registrar.register_tracked_schema_ids(schema_id_list)

        self.enable_meteorite = get_config().enable_meteorite
        self.enable_sensu = get_config().enable_sensu
        self.monitors = {}
        self._next_sensu_update = 0
        self._sensu_window = 0
        self._setup_monitors()

    @cached_property
    def _kafka_producer(self):
        if self.use_work_pool:
            return PooledKafkaProducer(
                self._set_kafka_producer_position,
                dry_run=self.dry_run
            )
        else:
            return LoggingKafkaProducer(
                self._set_kafka_producer_position,
                dry_run=self.dry_run
            )

    @property
    def client_type(self):
        """String identifying the client type."""
        return 'producer'

    def __enter__(self):
        # By default, the kafka producer is created lazily, and doesn't
        # actually do anything until it needs to.  This method is used here to
        # force the kafka producer to wake up, which will guarantee that
        # its initialized before it is used.  This is important, since without
        # it, checkpoint position data won't be passed to the producer until
        # the user starts publishing messages.
        self.wake()
        self.registrar.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.info("Closing producer...")
        try:
            self.close()
            logger.info("Producer closed")
        except:
            logger.exception("Failed to close the Producer.")
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

    def _setup_monitors(self):
        """This method sets up the meteorite monitor as well as the two sensu
        monitors, first for ttl, and second for delay.  The ttl monitor tracks
        the health of the producer and upstream heartbeat.  The delay monitor
        tracks whether the producer has fallen too far behind the upstream
        data"""

        try:
            from data_pipeline.tools.meteorite_wrappers import StatsCounter
            from data_pipeline.tools.sensu_alert_manager import SensuAlertManager
            from data_pipeline.tools.sensu_ttl_alerter import SensuTTLAlerter
        except ImportError:
            self.enable_meteorite = False
            self.enable_sensu = False
            return

        self.monitors["meteorite"] = StatsCounter(
            stat_counter_name=self.client_name,
            container_name=get_config().container_name,
            container_env=get_config().container_env
        )

        underscored_client_name = "_".join(self.client_name.split())
        # Sensu event dictionary parameters are described here:
        # http://pysensu-yelp.readthedocs.io/en/latest/index.html?highlight=send_event
        ttl_sensu_dict = {
            'name': "{0}_outage_check".format(underscored_client_name),
            'output': "{0} is back on track".format(self.client_name),
            'runbook': "y/datapipeline",
            'team': self.registrar.team_name,
            'page': get_config().sensu_page_on_critical,
            'status': 0,
            'ttl': "{0}s".format(get_config().sensu_ttl),
            'sensu_host': get_config().sensu_host,
            'source': "{0}_{1}".format(
                self.client_name,
                get_config().sensu_source
            ),
            'tip': "either the producer has died or there are no hearbeats upstream"
        }
        self._sensu_window = get_config().sensu_ping_window_seconds
        self.monitors["sensu_ttl"] = SensuTTLAlerter(
            sensu_event_info=ttl_sensu_dict,
            enable=self.enable_sensu
        )

        delay_sensu_dict = copy.deepcopy(ttl_sensu_dict)
        delay_sensu_dict.update({
            'name': "{0}_delay_check".format(underscored_client_name),
            'alert_after': get_config().sensu_alert_after_seconds,
        })
        disable_sensu = not self.enable_sensu
        SENSU_DELAY_ALERT_INTERVAL_SECONDS = 30
        self.monitors["sensu_delay"] = SensuAlertManager(
            SENSU_DELAY_ALERT_INTERVAL_SECONDS,
            self.client_name,
            delay_sensu_dict,
            get_config().max_producer_delay_seconds,
            disable=disable_sensu
        )

    def publish(self, message, timestamp=None):
        """Adds the message to the buffer to be published.  Messages are
        published after a number of messages are accumulated or after a
        slight time delay, whichever passes first.  Passing a message to
        publish does not guarantee that it will be successfully published into
        Kafka.

        **TODO(DATAPIPE-155|justinc)**:

        * Point to information about the message accumulation and time
          delay config.
        * Include information about checking which messages have actually
          been published.

        Args:
            message (data_pipeline.message.Message): message to publish
            timestamp (timezone aware timestamp): utc datetime of event
        """
        self._kafka_producer.publish(message)

        if self.enable_meteorite:
            self.monitors['meteorite'].process(message.topic)

        if self.enable_sensu and time.time() > self._next_sensu_update:
            self._next_sensu_update = time.time() + self._sensu_window
            self.monitors['sensu_ttl'].process()
            self.monitors['sensu_delay'].process(timestamp)

        self.monitor.record_message(message)
        self.registrar.update_schema_last_used_timestamp(
            message.schema_id,
            timestamp_in_milliseconds=long(1000 * time.time())
        )

    def ensure_messages_published(self, messages, topic_offsets):
        """This method should only be used when recovering after an unclean
        shutdown, and only if the upstream message source is persistent and can
        be rewound and replayed.  All messages produced since the last
        successful checkpoint should be passed as a list into this method,
        which will then ensure that each message has either already been
        published into Kafka, or will publish each message into Kafka.

        The call will block until all messages are published successfully.

        Immediately after calling this method, you should call
        :meth:`get_checkpoint_position_data` and persist the data.

        Args:
            messages (list of :class:`data_pipeline.message.Message`): List of
                messages to ensure are published.  The order of the messages
                matters, this code assumes that the messages are in the order
                they would have been published in.
            topic_offsets (dict of str to int): The topic offsets should be a
                dictionary containing the offset of the next message that would
                be published in each topic.  This should be in the format of
                :attr:`data_pipeline.position_data.PositionData.topic_to_kafka_offset_map`.

        Raises:
            PublicationUnensurableError: If any topics already have more messages
                published than would be published by this method, an assertion
                will fail.  This should never happen in practice.  If it
                does, it means that there has either been another publisher
                writing to the topic, which breaks the data pipeline contract,
                or there weren't enough messages passed into this method.  In
                either case, manual intervention will be required.  Note that
                in the event of a failure, some messages may have been
                published.
        """
        topic_messages_map = self._generate_topic_messages_map(messages)
        # raise_on_error must be set to False, otherwise this call will raise
        # an exception when any topic doesn't exist, preventing the topic from
        # ever being created in the context of ensure_messages_published
        topic_actual_published_count_map = (
            get_actual_published_messages_count(
                self._kafka_producer.kafka_client,
                topics=topic_messages_map.keys(),
                topic_tracked_offset_map=topic_offsets,
                raise_on_error=False
            )
        )

        # TODO(justinc|DATAPIPE-995): This can't be a set at this time, because
        # some data pipeline message fields aren't hashable - primarily the
        # upstream_position_info, which is a dict.
        already_published_messages = list()
        for topic, topic_messages in topic_messages_map.iteritems():
            # `get_actual_published_messages_count` only returns the message
            # count for topics that exist, so for non-existent topics, here it
            # sets the actual published message count to 0, i.e. high watermark
            # is 0.
            already_published_count = topic_actual_published_count_map.get(topic, 0)
            saved_offset = topic_offsets.get(topic, 0)

            info_to_log = dict(
                message="Attempting to ensure messages published",
                topic=topic,
                saved_offset=saved_offset,
                high_watermark=already_published_count + saved_offset,
                message_count=len(topic_messages),
                already_published_count=already_published_count
            )

            logger.info(json.dumps(info_to_log))

            if (
                already_published_count < 0 or
                already_published_count > len(topic_messages)
            ):
                # This is here primarily as a convenience to allow recovery
                # after logical errors.  It will result in breaking the
                # delivery guarantees.
                if get_config().force_recovery_from_publication_unensurable_error:
                    logger.critical(
                        "Forcing recovery from PublicationUnensurableError - "
                        "Intentionally Breaking Delivery Guarantees. "
                        "Turn force_recovery_from_publication_unensurable_error "
                        "off after recovery."
                    )
                    already_published_count = 0
                else:
                    raise PublicationUnensurableError()
            already_published_messages.extend(
                topic_messages[:already_published_count]
            )

        # Automatic flushing must be disabled while we're recovering, since
        # any partial flushing will result in state-saving callbacks being
        # triggered, which can cause downstream applications to update state
        # that really shouldn't be updated until all messages are published
        # successfully.
        position_tracker = self._kafka_producer.position_data_tracker
        with self._kafka_producer.disable_automatic_flushing():
            for message in messages:
                # We're recording already published messages here so that if
                # there's any ordering dependency related to state saving, we're
                # able to capture that.
                #
                # Concretely, imagine the last message has already been
                # published, and that messages come from a serial source like
                # db replication.  If we don't record the last message, even
                # though we're not actually publishing it, the state in the
                # producer will indicate the last message hasn't been published,
                # when we know that it has.  This breaks things if the
                # application crashes after this procedure, but before saving
                # again.
                if message in already_published_messages:
                    position_tracker.record_message(message)

                    topic_of_message = message.topic
                    already_published_count = topic_actual_published_count_map.get(
                        topic_of_message,
                        0
                    )
                    saved_offset = topic_offsets.get(topic_of_message, 0)
                    # This is required to update the high watermark for all the
                    # messages individually on the position tracker in-order to
                    # avoid offset in there from becoming stale.
                    position_tracker.update_high_watermark(
                        topic=message.topic,
                        offset=saved_offset,
                        message_count=already_published_count
                    )
                else:
                    self.publish(message)

            self.flush()

    def flush(self):
        """Block until all data pipeline messages have been
        successfully published into Kafka.
        """
        self._kafka_producer.flush_buffered_messages()

    def close(self):
        """Closes the producer, flushing all buffered messages into Kafka.
        Calling this method directly is not recommended, instead, use the
        producer as a context manager::

            with Producer() as producer:
                producer.publish(message)
                ...
                producer.publish(message)
        """
        self.registrar.stop()
        self.monitor.close()
        self._kafka_producer.close()
        assert len(multiprocessing.active_children()) == 0

    def get_checkpoint_position_data(self):
        """
        returns:
            PositionData: `PositionData` structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        return self.position_data

    def wake(self):
        """The synchronous producer has no mechanism to flush messages on its
        own, in the absence of other messages being published.  Consequently,
        if there are gaps where messages aren't published, this method should
        be called to allow the producer to flush its buffers if it needs to.

        If messages aren't published at least every 250ms, this method should
        be called about that often, to ensure that messages don't sit in the
        buffer for longer than that.

        Example::

            If the upstream messages are coming in slowly, or there can be gaps,
            call wake periodically so the producer has a change to publish
            messages::

                with Producer() as producer:
                    while True:
                        # if no new message arrive after 100ms, wake up the
                        # producer.
                        try:
                            message = slow_queue.get(block=True, timeout=0.1)
                            producer.publish(message)
                        except Empty:
                            producer.wake()
        """
        self._kafka_producer.wake()

    def _set_kafka_producer_position(self, position_data):
        """Called periodically to update the producer with position data.  This
        is expected to be called at least once when the KafkaProducer is started,
        and whenever messages are successfully published.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        self.position_data = position_data
        if self.position_data_callback:
            self.position_data_callback(position_data)

    def _generate_topic_messages_map(self, messages):
        topic_messages_map = defaultdict(list)
        for message in messages:
            topic_messages_map[message.topic].append(message)
        return topic_messages_map
