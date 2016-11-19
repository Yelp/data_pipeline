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

import time
from collections import defaultdict
from collections import namedtuple
from contextlib import contextmanager

from cached_property import cached_property
from kafka import create_message
from kafka import KafkaClient
from kafka.common import ProduceRequest

from data_pipeline._position_data_tracker import PositionDataTracker
from data_pipeline._producer_retry import RetryHandler
from data_pipeline._retry_util import ExpBackoffPolicy
from data_pipeline._retry_util import MaxRetryError
from data_pipeline._retry_util import Predicate
from data_pipeline._retry_util import retry_on_condition
from data_pipeline._retry_util import RetryPolicy
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope


_EnvelopeAndMessage = namedtuple("_EnvelopeAndMessage", ["envelope", "message"])
logger = get_config().logger


# prepare needs to be in the module top level so it can be serialized for
# multiprocessing
def _prepare(envelope_and_message):
    try:
        kwargs = {}
        if envelope_and_message.message.keys:
            kwargs['key'] = envelope_and_message.message.encoded_keys
        return create_message(
            envelope_and_message.envelope.pack(envelope_and_message.message),
            **kwargs
        )
    except:
        logger.exception('Prepare failed')
        raise


class KafkaProducer(object):
    """The KafkaProducer deals with buffering messages that need to be published
    into Kafka, preparing them for publication, and ultimately publishing them.

    Args:
        producer_position_callback (function): The producer position callback
            is called when the KafkaProducer is instantiated, and every time
            messages are published to notify the producer of current position
            information of successfully published messages.
        dry_run (Optional[bool]): When dry_run mode is on, the producer won't
            talk to real KafKa topic, nor to real Schematizer.  Default to False.
    """
    @cached_property
    def envelope(self):
        return Envelope()

    def __init__(self, producer_position_callback, dry_run=False):
        self.producer_position_callback = producer_position_callback
        self.dry_run = dry_run
        self.kafka_client = KafkaClient(get_config().cluster_config.broker_list)
        self.position_data_tracker = PositionDataTracker()
        self._reset_message_buffer()
        self.skip_messages_with_pii = get_config().skip_messages_with_pii
        self._publish_retry_policy = RetryPolicy(
            ExpBackoffPolicy(with_jitter=True),
            max_retry_count=get_config().producer_max_publish_retry_count
        )
        self._automatic_flush_enabled = True

    @contextmanager
    def disable_automatic_flushing(self):
        """Prevents the producer from flushing automatically (e.g. for timeouts
        or batch size) while the context manager is open.
        """
        try:
            self._automatic_flush_enabled = False
            yield
        finally:
            self._automatic_flush_enabled = True

    def wake(self):
        """Should be called periodically if we're not otherwise waking up by
        publishing, to ensure that messages are actually published.
        """
        # if we haven't woken up in a while, we may need to flush messages
        self._flush_if_necessary()

    def publish(self, message):
        if message.contains_pii and self.skip_messages_with_pii:
            logger.info(
                "Skipping a PII message - "
                "uuid hex: {0}, "
                "schema_id: {1}, "
                "timestamp: {2}, "
                "type: {3}".format(
                    message.uuid_hex,
                    message.schema_id,
                    message.timestamp,
                    message.message_type.name
                )
            )
            return
        self._add_message_to_buffer(message)
        self.position_data_tracker.record_message_buffered(message)
        self._flush_if_necessary()

    def flush_buffered_messages(self):
        produce_method = (self._publish_produce_requests_dry_run
                          if self.dry_run else self._publish_produce_requests)
        produce_method(self._generate_produce_requests())
        self._reset_message_buffer()

    def close(self):
        self.flush_buffered_messages()
        self.kafka_client.close()

    def _publish_produce_requests(self, requests):
        """It will try to publish all the produce requests for topics, and
        retry a number of times until either all the requests are successfully
        published or it can no longer retry, in which case, the exception will
        be thrown.

        Each time the requests that are successfully published in the previous
        round will be removed from the requests and won't be published again.
        """
        unpublished_requests = list(requests)
        retry_handler = RetryHandler(self.kafka_client, unpublished_requests)

        def has_requests_to_be_sent():
            return bool(retry_handler.requests_to_be_sent)

        retry_handler = retry_on_condition(
            retry_policy=self._publish_retry_policy,
            retry_conditions=[Predicate(has_requests_to_be_sent)],
            func_to_retry=self._publish_requests,
            use_previous_result_as_param=True,
            retry_handler=retry_handler
        )
        if retry_handler.has_unpublished_request:
            raise MaxRetryError(last_result=retry_handler)

    def _publish_requests(self, retry_handler):
        """Main function to publish message requests.  This function is wrapped
        with retry function and will be retried based on specified retry policy

        Args:
            retry_handler: :class:`data_pipeline._producer_retry.RetryHandler`
                that determines which messages should be retried next time.
        """
        if not retry_handler.requests_to_be_sent:
            return retry_handler

        responses = self._try_send_produce_requests(
            retry_handler.requests_to_be_sent
        )

        retry_handler.update_requests_to_be_sent(
            responses,
            self.position_data_tracker.topic_to_kafka_offset_map
        )
        self._record_success_requests(retry_handler.success_topic_stats_map)
        return retry_handler

    def _try_send_produce_requests(self, requests):
        # Either it throws exceptions and none of them succeeds, or it returns
        # responses of all the requests (success or fail response).
        try:
            return self.kafka_client.send_produce_request(
                payloads=requests,
                acks=get_config().kafka_client_ack_count,
                fail_on_error=False
            )
        except Exception:
            # Exceptions like KafkaUnavailableError, LeaderNotAvailableError,
            # UnknownTopicOrPartitionError, etc., are not controlled by
            # `fail_on_error` flag and could be thrown from the kafka client,
            # and fail all the requests.  We will retry all the requests until
            # either all of them are successfully published or it exceeds the
            # maximum retry criteria.
            return []

    def _record_success_requests(self, success_topic_stats_map):
        for topic_partition, stats in success_topic_stats_map.iteritems():
            topic = topic_partition.topic_name
            assert stats.message_count == len(self.message_buffer[topic])
            self.position_data_tracker.record_messages_published(
                topic=topic,
                offset=stats.original_offset,
                message_count=stats.message_count
            )
            self.message_buffer.pop(topic)

    def _publish_produce_requests_dry_run(self, requests):
        for request in requests:
            self._publish_single_request_dry_run(request)

    def _publish_single_request_dry_run(self, request):
        topic = request.topic
        message_count = len(request.messages)
        self.position_data_tracker.record_messages_published(
            topic,
            -1,
            message_count
        )

    def _is_ready_to_flush(self):
        time_limit = get_config().kafka_producer_flush_time_limit_seconds
        return (self._automatic_flush_enabled and (
            (time.time() - self.start_time) >= time_limit or
            self.message_buffer_size >= get_config().kafka_producer_buffer_size
        ))

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
        if not hasattr(self, 'message_buffer_size') or self.message_buffer_size > 0:
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
        except MaxRetryError as e:
            logger.exception(
                "Failed to publish all produce requests. {0}".format(repr(e))
            )
            raise

    def _reset_message_buffer(self):
        logger.info("Resetting message buffer for success requests.")
        super(LoggingKafkaProducer, self)._reset_message_buffer()

    def _publish_single_request_dry_run(self, request):
        super(LoggingKafkaProducer, self)._publish_single_request_dry_run(request)
        logger.debug("dry_run mode: Would have published {0} messages to {1}".format(
            len(request.messages),
            request.topic
        ))
