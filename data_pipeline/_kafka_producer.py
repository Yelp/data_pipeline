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


class PublishMessagesFailedError(Exception):
    pass


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
        self.delay_between_retries_in_sec = get_config().delay_between_producer_retries_in_sec
        self.max_retry_count = get_config().producer_max_retry_count
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
        """It will try to publish all the produce requests for topics, and
        retry a number of times until either all the requests are successfully
        published or it exceeds the maximum number of retries. If it exceeds
        the maximum number of retries, the exception will be thrown.

        Each time the requests that are successfully published in the previous
        round will be removed from the requests and won't be published again.
        """
        if not requests:
            return

        retried_count = 0
        unpublished_requests = list(requests)
        responses = []
        published_messages_count = 0

        while retried_count < self.max_retry_count:
            try:
                responses = self.kafka_client.send_produce_request(
                    payloads=unpublished_requests,
                    acks=1,  # Written to disk on master
                    fail_on_error=False  # disable raising exception when retrying
                )
            except Exception:
                # Exceptions like KafkaUnavailableError, LeaderNotAvailableError,
                # UnknownTopicOrPartitionError, etc., are not controlled by
                # `fail_on_error` flag and could be thrown from the kafka
                # client, which fail all the requests. We will retry all the
                # requests until either all of them are successfully published
                # or it exceeds maximum number of retries.
                # TODO [DATAPIPE-325|clin] Have nicer handling in such case
                responses = []

            success_responses = [r for r in responses if self._is_success_response(r)]
            published_messages_count = self._record_success_responses(
                success_responses,
                current_published_msgs_count=published_messages_count
            )

            unpublished_requests = self._get_failed_requests(
                unpublished_requests,
                success_responses
            )
            if not unpublished_requests:
                break  # All the requests are successfully published. Done.

            retried_count += 1
            time.sleep(self.delay_between_retries_in_sec)

        # Return only when all the messages are published
        # TODO [DATAPIPE-325|clin] Have nicer handling in such case
        if (unpublished_requests or
                published_messages_count != self.message_buffer_size):
            self._assert_failed_requests(
                unpublished_requests,
                responses,
                published_messages_count
            )

    def _is_success_response(self, response):
        """Three possible responses: success, failure, nothing (missing).
        A successful response is the one that is not an exception, such
        as FailedPayloadsError, and has error == 0. Otherwise, it is an
        unsuccessful response.
        """
        return (response and not isinstance(response, Exception) and
                response.error == 0)

    def _record_success_responses(self, success_responses, current_published_msgs_count):
        new_published_msgs_count = current_published_msgs_count
        for response in success_responses:
            self.position_data_tracker.record_messages_published(
                response.topic,
                response.offset,
                len(self.message_buffer[response.topic])
            )
            new_published_msgs_count += len(self.message_buffer[response.topic])
        return new_published_msgs_count

    def _get_failed_requests(self, requests, success_responses):
        success_topics_and_partitions = set(
            (r.topic, r.partition) for r in success_responses
        )
        return [r for r in requests
                if not (r.topic, r.partition) in success_topics_and_partitions]

    def _assert_failed_requests(self, failed_requests, responses, published_msgs_count):
        failed_responses = [r for r in responses
                            if not self._is_success_response(r)]
        raise PublishMessagesFailedError(
            '{0} messages buffered. {1} messages published. '
            'Failed requests: {2}. Failed responses: {3}.'.format(
                self.message_buffer_size,
                published_msgs_count,
                repr(failed_requests),
                repr(failed_responses)
            )
        )

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
        try:
            super(LoggingKafkaProducer, self)._publish_produce_requests(requests)
            logger.info("All messages published successfully")
        except PublishMessagesFailedError as e:
            logger.exception(
                "Failed to publish all produce requests. {0}".format(repr(e))
            )
            raise

    def _reset_message_buffer(self):
        logger.info("Resetting message buffer")
        super(LoggingKafkaProducer, self)._reset_message_buffer()
