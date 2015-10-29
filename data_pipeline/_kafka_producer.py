# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import defaultdict
from collections import namedtuple

from cached_property import cached_property
from kafka import create_message
from kafka.common import LeaderNotAvailableError
from kafka.common import ProduceRequest
from yelp_kafka import error
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
        kwargs = {}
        if envelope_and_message.message.keys:
            kwargs['key'] = envelope_and_message.envelope.pack_keys(
                envelope_and_message.message.keys
            )

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
        published or it can no longer retry, in which case, the exception will
        be thrown.

        Each time the requests that are successfully published in the previous
        round will be removed from the requests and won't be published again.
        """
        # TODO [DATAPIPE-325|clin] Have nicer handling
        unpublished_requests = list(requests)
        published_msgs_count = 0

        def has_unpublished_requests():
            return bool(unpublished_requests)

        failed_requests, published_count, excluded_requests = retry_on_condition(
            retry_policy=self._publish_retry_policy,
            retry_conditions=[Predicate(has_unpublished_requests)],
            func_to_retry=self._publish_requests,
            use_previous_result_as_param=True,
            unpublished_requests=unpublished_requests,
            published_msgs_count=published_msgs_count,
            excluded_requests=[]
        )
        if excluded_requests:
            raise MaxRetryError(
                last_result=(failed_requests, published_count, excluded_requests)
            )

    def _publish_requests(self, unpublished_requests, published_msgs_count,
                          excluded_requests):
        """Main function to publish message requests.  This function is wrapped
        with retry function and will be retried based on specified retry policy

        Args:
            unpublished_requests: List of requests to be published
            published_msgs_count: Number of messages successfully published so far
            excluded_requests: List of requests that fail previously and cannot
                be retried.
        """
        if not unpublished_requests:
            return unpublished_requests, published_msgs_count, excluded_requests

        responses = self._try_send_produce_requests(unpublished_requests)

        published_count, failed_requests = self._process_success_responses(
            unpublished_requests,
            responses,
        )
        published_msgs_count += published_count

        published_count, retry_request_map = self._verify_failed_requests(
            failed_requests,
        )
        published_msgs_count += published_count
        excluded_requests += retry_request_map[False]

        self._update_unpublished_requests(
            unpublished_requests,
            retry_request_map[True]
        )
        return unpublished_requests, published_msgs_count, excluded_requests

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

    def _process_success_responses(self, requests, responses):
        success_resps = [r for r in responses if self._is_success_response(r)]
        published_msgs_count = self._record_success_requests(
            [(r.topic, r.offset) for r in success_resps],
        )

        failed_requests = self._get_failed_requests(requests, success_resps)
        return published_msgs_count, failed_requests

    def _is_success_response(self, response):
        """In our case, the response is either ProduceResponse (success) or
        FailedPayloadsError (failed) if no other exception is thrown.  The
        ProduceResponse should have error == 0.
        """
        return not isinstance(response, Exception) and response.error == 0

    def _get_failed_requests(self, requests, success_responses):
        success_topics_and_partitions = set(
            (r.topic, r.partition) for r in success_responses
        )
        return [r for r in requests
                if (r.topic, r.partition) not in success_topics_and_partitions]

    def _record_success_requests(self, success_topics_and_offsets):
        published_msgs_count = 0
        for topic, offset in success_topics_and_offsets:
            self.position_data_tracker.record_messages_published(
                topic=topic,
                offset=offset,
                message_count=len(self.message_buffer[topic])
            )
            published_msgs_count += len(self.message_buffer[topic])
            self.message_buffer.pop(topic)
        return published_msgs_count

    def _verify_failed_requests(self, requests):
        """Verify if the requests actually fail by checking the high watermark
        of the corresponding topics.  If the high watermark of a topic matches
        the number of messages in the request, the request is considered as
        successfully published, and the offset is saved in the position_data_tracker.

        If the high watermark data cannot be retrieved and it is not due to
        missing topic/partition, the request will be considered as failed but
        won't be retried because it cannot determine whether the messages are
        actually published.  Otherwise, the requests will be retried.
        """
        # `get_topics_watermarks` fails all the topics if any partition leader
        # is not available, so here it checks each topic individually.
        retry_requests_map = {True: [], False: []}
        published_msgs_count = 0
        topic_offset_map = self.position_data_tracker.topic_to_kafka_offset_map
        for request in requests:
            try:
                topic = request.topic
                published_msgs_count_map = self.get_actual_published_messages_count(
                    [topic],
                    topic_tracked_offset_map=topic_offset_map
                )
                published_count = published_msgs_count_map.get(topic)

                if len(request.messages) != published_count:
                    retry_requests_map[True].append(request)
                    continue

                published_msgs_count += published_count
                offset = published_count + topic_offset_map[topic]
                self._record_success_requests([(topic, offset)])

            except (error.UnknownTopic, error.UnknownPartitions):
                # May be due to the topic doesn't exist yet or stale metadata;
                # try to load the metadata for the latter case
                should_retry = self._try_load_topic_metadata(request)
                retry_requests_map[should_retry].append(request)

            except Exception:
                # Unable to get the high watermark of this topic; do not retry
                # this request since it's unclear if the messages are actually
                # successfully published.
                retry_requests_map[False].append(request)
        return published_msgs_count, retry_requests_map

    def _try_load_topic_metadata(self, request):
        """Try to load the metadata of the topic of the given request.  It
        returns True if the request should be retried, and False otherwise.
        """
        try:
            self.kafka_client.load_metadata_for_topics(request.topic)
            return True
        except LeaderNotAvailableError:
            # Topic doesn't exist yet but the broker is configured to create
            # the topic automatically.
            return True
        except Exception:
            return False

    def get_actual_published_messages_count(
        self,
        topics,
        topic_tracked_offset_map,
        raise_on_error=True
    ):
        """Get the actual number of published messages of specified topics.

        Args:
            topics ([str]): List of topic names to get message count
            topic_tracked_offset_map (dict(str, int)): dictionary which
                contains each topic and its current stored offset value.
            raise_on_error (Optional[bool]): if False,  the function ignores
                missing topics and missing partitions. It still may fail on
                the request send.  Default to True.

        Returns:
            dict(str, int): Each topic and its actual published messages count
                since last offset.  If a topic or partition is missing when
                `raise_on_error` is False, the returned dict will not contain
                the missing topic.

        Raises:
            :class:`~yelp_kafka.error.UnknownTopic`: upon missing topics and
                raise_on_error=True
            :class:`~yelp_kafka.error.UnknownPartition`: upon missing partitions
            and raise_on_error=True
            FailedPayloadsError: upon send request error.
        """
        topic_watermarks = get_topics_watermarks(
            self.kafka_client,
            topics,
            raise_on_error=raise_on_error
        )

        topic_to_published_msgs_count = {}
        for topic, partition_offsets in topic_watermarks.iteritems():
            high_watermark = partition_offsets[0].highmark
            offset = topic_tracked_offset_map.get(topic, 0)
            topic_to_published_msgs_count[topic] = high_watermark - offset

        return topic_to_published_msgs_count

    def _update_unpublished_requests(self, unpublished_requests, retry_requests):
        # This is mainly for the retry condition `has_unpublished_requests` in
        # the function `_publish_produce_requests`, which checks if it needs to
        # retry sending the requests.
        topics_and_partitions = set((r.topic, r.partition) for r in retry_requests)
        for i in range(len(unpublished_requests) - 1, -1, -1):
            r = unpublished_requests[i]
            if (r.topic, r.partition) not in topics_and_partitions:
                unpublished_requests.remove(unpublished_requests[i])

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
