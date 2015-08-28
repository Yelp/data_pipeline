# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import defaultdict
from collections import namedtuple

from cached_property import cached_property
from kafka import create_message
from kafka.common import ProduceRequest
from yelp_kafka.offsets import get_topics_watermarks

from data_pipeline._position_data_tracker import PositionDataTracker
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
        self.kafka_client = get_config().kafka_client
        self.position_data_tracker = PositionDataTracker()
        self._reset_message_buffer()
        self.skip_messages_with_pii = get_config().skip_messages_with_pii
        self._publish_retry_policy = RetryPolicy(
            ExpBackoffPolicy(with_jitter=True),
            max_retry_count=get_config().producer_max_publish_retry_count
        )

    def wake(self):
        """Should be called periodically if we're not otherwise waking up by
        publishing, to ensure that messages are actually published.
        """
        # if we haven't woken up in a while, we may need to flush messages
        self._flush_if_necessary()

    def publish(self, message):
        if self.skip_messages_with_pii and message.contains_pii:
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
        published or it can no longer retry, in which case., the exception will
        be thrown.

        Each time the requests that are successfully published in the previous
        round will be removed from the requests and won't be published again.
        """
        # TODO [DATAPIPE-325|clin] Have nicer handling
        unpublished_requests = list(requests)
        published_msgs_count = 0

        def has_unpublished_requests():
            return bool(unpublished_requests)

        retry_on_condition(
            retry_policy=self._publish_retry_policy,
            retry_conditions=[Predicate(has_unpublished_requests)],
            func_to_retry=self._publish_requests,
            use_previous_result_as_param=True,
            unpublished_requests=unpublished_requests,
            published_messages_count=published_msgs_count
        )

    def _publish_requests(self, unpublished_requests, published_messages_count):
        if not unpublished_requests:
            return unpublished_requests, published_messages_count

        # Either it throws exceptions and none of them succeeds, or it returns
        # responses of all the requests (success or fail response).
        try:
            responses = self.kafka_client.send_produce_request(
                payloads=unpublished_requests,
                acks=get_config().kafka_client_ack_count,
                fail_on_error=False
            )
        except Exception:
            # Exceptions like KafkaUnavailableError, LeaderNotAvailableError,
            # UnknownTopicOrPartitionError, etc., are not controlled by
            # `fail_on_error` flag and could be thrown from the kafka
            # client, which fail all the requests. We will retry all the
            # requests until either all of them are successfully published
            # or it exceeds maximum retry criteria.
            responses = []

        published_messages_count = self._process_success_responses(
            unpublished_requests,
            responses,
            current_published_msgs_count=published_messages_count
        )
        published_messages_count = self._verify_failed_requests(
            unpublished_requests,
            current_published_msgs_count=published_messages_count
        )
        return unpublished_requests, published_messages_count

    def _process_success_responses(
        self,
        requests,
        responses,
        current_published_msgs_count
    ):
        success_resps = [r for r in responses if self._is_success_response(r)]
        self._extract_success_requests(
            requests,
            self._is_request_with_success_response(success_resps)
        )
        return self._record_success_requests(
            [(r.topic, r.offset) for r in success_resps],
            current_published_msgs_count=current_published_msgs_count
        )

    def _is_success_response(self, response):
        """In our case, the response is either ProduceResponse (success) or
        FailedPayloadsError (failed) if no other exception is thrown.  The
        ProduceResponse should have error == 0.
        """
        return not isinstance(response, Exception) and response.error == 0

    def _is_request_with_success_response(self, success_responses):
        success_topics_and_partitions = set(
            (r.topic, r.partition) for r in success_responses
        )

        def predicate(req):
            return (req.topic, req.partition) in success_topics_and_partitions
        return predicate

    def _extract_success_requests(self, requests, predicate_for_success):
        success_requests = []
        for r in requests:
            if predicate_for_success(r):
                success_requests.append(r)
                requests.remove(r)
        return success_requests

    def _record_success_requests(
        self,
        success_topics_and_offsets,
        current_published_msgs_count
    ):
        new_published_msgs_count = current_published_msgs_count
        for topic, offset in success_topics_and_offsets:
            self.position_data_tracker.record_messages_published(
                topic=topic,
                offset=offset,
                message_count=len(self.message_buffer[topic])
            )
            new_published_msgs_count += len(self.message_buffer[topic])
            self.message_buffer.pop(topic)
        return new_published_msgs_count

    def _verify_failed_requests(self, requests, current_published_msgs_count):
        topic_actual_published_count_map = self.get_actual_published_messages_count(
            topics=[r.topic for r in requests],
            topic_to_tracked_offset_map=self.position_data_tracker.topic_to_kafka_offset_map,
            raise_on_error=False
        )
        success_reqs = self._extract_success_requests(
            requests,
            self._is_false_fail_request(topic_actual_published_count_map)
        )
        return self._record_success_requests(
            [(r.topic, topic_actual_published_count_map[r.topic] + len(r.messages))
             for r in success_reqs],
            current_published_msgs_count=current_published_msgs_count
        )

    def _is_false_fail_request(self, topic_actual_published_msgs_count_map):
        def predicate(request):
            return (len(request.messages) ==
                    topic_actual_published_msgs_count_map.get(request.topic))
        return predicate

    def get_actual_published_messages_count(
        self,
        topics,
        topic_to_tracked_offset_map,
        raise_on_error=True
    ):
        """Get the actual number of published messages of specified topics.

        Args:
            topics ([str]): List of topic names to get message count
            topic_to_tracked_offset_map (dict(str, int)): dictionary which
                contains each topic and its current stored offset value.
            raise_on_error (Optional[bool]): if False,  the function ignores
                missing topics and missing partitions. It still may fail on
                the request send.  Default to True.

        Returns:
            dict(str, int): Each topic and its actual published messages count
                since last offset.  When `raise_on_error` is False, it is
                possible that not all the specified topics are in the returned
                dictionary due to non-existent topic or non network errors.

        Raises:
            :class:`~yelp_kafka.error.UnknownTopic`: upon missing topics and
                raise_on_error=True
            :class:`~yelp_kafka.error.UnknownPartition`: upon missing partitions
            and raise_on_error=True
            FailedPayloadsError: upon send request error.
        """
        topic_to_high_watermark_map = self._get_topics_high_watermarks(
            topics,
            raise_on_error=raise_on_error
        )
        topic_to_published_msgs_count = {}
        for topic, high_watermark in topic_to_high_watermark_map.iteritems():
            offset = topic_to_tracked_offset_map.get(topic, 0)
            topic_to_published_msgs_count[topic] = high_watermark - offset

        return topic_to_published_msgs_count

    def _get_topics_high_watermarks(self, topics, raise_on_error=True):
        topics_watermarks = get_topics_watermarks(
            self.kafka_client,
            topics,
            raise_on_error=raise_on_error
        )
        return {
            topic: partition_offsets[0].highmark
            for topic, partition_offsets in topics_watermarks.iteritems()
        }

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
        return (
            (time.time() - self.start_time) >= time_limit or
            self.message_buffer_size >= get_config().kafka_producer_buffer_size
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
