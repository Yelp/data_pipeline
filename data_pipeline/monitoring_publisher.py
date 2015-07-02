# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing

from cached_property import cached_property

from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline._pooled_kafka_producer import PooledKafkaProducer
from data_pipeline.config import get_config


logger = get_config().logger


class MonitoringPublisher(object):

    @cached_property
    def _kafka_producer(self):
        if self.use_work_pool:
            return PooledKafkaProducer(self._notify_messages_published)
        else:
            return LoggingKafkaProducer(self._notify_messages_published)

    def __init__(self, use_work_pool=False):
        self.use_work_pool = use_work_pool

    def __enter__(self):
        # By default, the kafka producer is created lazily, and doesn't
        # actually do anything until it needs to.  This method is used here to
        # force the kafka producer to wake up, which will guarantee that
        # its initialized before it is used.  This is important, since without
        # it, checkpoint position data won't be passed to the producer until
        # the user starts publishing messages.
        self.wake()

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

    def publish(self, message):
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
        """
        self._kafka_producer.publish(message)

    def ensure_messages_published(self, messages, topic_offsets):
        """This method should only be used when recovering after an unclean
        shutdown, and only if the upstream message source is persistent and can
        be rewound and replayed.  All messages produced since the last
        successful checkpoint should be passed as a list into this method,
        which will then ensure that each message has either already been
        published into Kafka, or will publish each message into Kafka.

        The call will block until all messages are published successfully.

        Args:
            messages (list of :class:`data_pipeline.message.Message`): List of
                messages to ensure are published.
            topic_offsets (dict of str to int): The topic offsets should be a
                dictionary containing the offset of the next message that would
                be published in each topic.  This should be in the format of
                :attr:`data_pipeline.position_data.PositionData.topic_to_kafka_offset_map`.
        """
        # TODO(DATAPIPE-164|justinc): Implement this
        raise NotImplementedError

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

    def _notify_messages_published(self, position_data):
        """Called to notify the producer of successfully published messages.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        logger.debug("Producer notified of new messages")
        self.position_data = position_data
