# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import defaultdict
from collections import namedtuple

from cached_property import cached_property
from kafka import create_message
from kafka.common import ProduceRequest

from data_pipeline._position_data_tracker import PositionDataTracker
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope


_EnvelopeAndMessage = namedtuple("_EnvelopeAndMessage", ["envelope", "message"])
logger = get_config().logger


# prepare needs to be in the module top level so it can be serialized for
# multiprocessing
def _prepare(envelope_and_message):
    try:
        return create_message(
            envelope_and_message.envelope.pack(envelope_and_message.message)
        )
    except:
        logger.exception('Prepare failed')
        raise


class KafkaProducer(object):
    """The KafkaProducer deals with buffering messages that need to be published
    into Kafka, preparing them for publication, and ultimately publishing them.

    Args:
      producer_position_callback (function): The producer position callback is
        called when the KafkaProducer is instantiated, and every time messages
        are published to notify the producer of current position information of
        successfully published messages.
    """
    message_limit = 5000
    time_limit = 0.1

    @cached_property
    def envelope(self):
        return Envelope()

    def __init__(self, producer_position_callback, dry_run=False):
        self.producer_position_callback = producer_position_callback
        self.dry_run = dry_run
        self.kafka_client = get_config().kafka_client
        self.position_data_tracker = PositionDataTracker()
        self._reset_message_buffer()

    def wake(self):
        """Should be called periodically if we're not otherwise waking up by
        publishing, to ensure that messages are actually published.
        """
        # if we haven't woken up in a while, we may need to flush messages
        self._flush_if_necessary()

    def publish(self, message):
        self._add_message_to_buffer(message)
        self.position_data_tracker.record_message_buffered(message)
        self._flush_if_necessary()

    def flush_buffered_messages(self):
        produce_method = self._publish_produce_requests_dry_run if self.dry_run else self._publish_produce_requests
        produce_method(self._generate_produce_requests())
        self._reset_message_buffer()

    def close(self):
        self.flush_buffered_messages()
        self.kafka_client.close()

    def _publish_produce_requests(self, requests):
        # TODO(DATAPIPE-149|justinc): This should be a loop, where on each
        # iteration all produce requests for topics that succeeded are removed,
        # and all produce requests that failed are retried.  If all haven't
        # succeeded after a few tries, this should blow up.
        try:
            published_messages_count = 0
            responses = self.kafka_client.send_produce_request(
                payloads=requests,
                acks=1  # Written to disk on master
            )
            for response in responses:
                # TODO(DATAPIPE-149|justinc): This won't work if the error code
                # is non-zero
                self.position_data_tracker.record_messages_published(
                    response.topic,
                    response.offset,
                    len(self.message_buffer[response.topic])
                )
                published_messages_count += len(self.message_buffer[response.topic])
            # Don't let this return if we didn't publish all the messages
            assert published_messages_count == self.message_buffer_size
        except:
            logger.exception("Produce failed... fix in DATAPIPE-149")
            raise

    def _publish_produce_requests_dry_run(self, requests):
        for request in requests:
            topic = request.topic
            message_count = len(request.messages)
            self.position_data_tracker.record_messages_published(
                topic,
                -1,
                message_count
            )
            logger.debug("dry_run mode: Would have published {0} messages to {1}".format(
                message_count,
                topic
            ))

    def _is_ready_to_flush(self):
        return (
            (time.time() - self.start_time) >= self.time_limit or
            self.message_buffer_size >= self.message_limit
        )

    def _flush_if_necessary(self):
        if self._is_ready_to_flush():
            self.flush_buffered_messages()

    def _add_message_to_buffer(self, message):
        topic = message.topic
        message = self._prepare_message(message)

        self.message_buffer[topic].append(message)
        self.message_buffer_size += 1

    def _generate_produce_requests(self):
        return [
            ProduceRequest(topic=topic, partition=0, messages=messages)
            for topic, messages in self._generate_prepared_topic_and_messages()
        ]

    def _generate_prepared_topic_and_messages(self):
        return self.message_buffer.iteritems()

    def _prepare_message(self, message):
        return _prepare(_EnvelopeAndMessage(envelope=self.envelope, message=message))

    def _reset_message_buffer(self):
        self.producer_position_callback(self.position_data_tracker.get_position_data())

        self.start_time = time.time()
        self.message_buffer = defaultdict(list)
        self.message_buffer_size = 0


class LoggingKafkaProducer(KafkaProducer):
    def _publish_produce_requests(self, requests):
        logger.info(
            "Flushing buffered messages - requests={0}, messages={1}".format(
                len(requests), self.message_buffer_size
            )
        )
        super(LoggingKafkaProducer, self)._publish_produce_requests(requests)
        logger.info("All messages published successfully")

    def _reset_message_buffer(self):
        logger.info("Resetting message buffer")
        super(LoggingKafkaProducer, self)._reset_message_buffer()
