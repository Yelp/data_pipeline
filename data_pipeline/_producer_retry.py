# -*- coding: utf-8 -*-
"""
This module contains classes that implement retry logic that provides various
publish guarantees.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from enum import Enum
from kafka.common import LeaderNotAvailableError
from yelp_kafka import error

from data_pipeline._kafka_util import get_actual_published_messages_count
from data_pipeline.config import get_config


_TopicPartition = namedtuple('_TopicOffset', ['topic_name', 'partition'])


class _Stats(namedtuple('_Stats', ['offset', 'message_count'])):

    def __add__(self, other):
        if type(other) is not type(self):
            raise TypeError("Unable to add non _Stats type object.")
        return _Stats(
            self.offset + other.offset,
            self.message_count + other.message_count
        )


class PublishGuaranteeEnum(Enum):

    exact_once = 0
    at_least_once = 1


class RetryHandler(object):
    """The class tracks the message publishing statistics in each retry,
    such as topic offset, number of published messages, etc., and determines
    which messages should be retried based on specified publishing guarantee.
    """

    def __init__(
        self,
        requests,
        publish_guarantee=PublishGuaranteeEnum.exact_once,
        kafka_client=None
    ):
        self.unpublished_requests = requests
        self.publish_guarantee = publish_guarantee
        self.success_topic_stats_map = {}
        self.success_topic_accum_stats_map = {}
        self.kafka_client = kafka_client or get_config().kafka_client

    def update_unpublished_requests(self, responses, topic_offsets=None):
        """Update stats from the responses of the publishing requests and
        determine which messages should be retried.

        Args:
            responses (kafka.common.FetchResponse or kafka.common.KafkaError):
                responses of the requests that publish messages to kafka topics
            topic_offsets (Optional[dict]): offset of each topic tracked by the
                producer so far.  It is used for exact-once publishing guarantee.
        """
        self.success_topic_stats_map = {}

        failed_requests = self._update_success_requests_stats(
            self.unpublished_requests,
            responses
        )
        if self.publish_guarantee == PublishGuaranteeEnum.exact_once:
            failed_requests = self._verify_failed_requests(
                failed_requests,
                topic_offsets
            )

        self.unpublished_requests = failed_requests

    def _update_success_requests_stats(self, requests, responses):
        """Update publish stats of successful requests and return the list of
        requests that do not have success responses.
        """
        success_responses = {
            (r.topic, r.partition): r
            for r in responses if self._is_success_response(r)
        }

        failed_requests = []
        for request in requests:
            topic, partition = request.topic, request.partition

            response = success_responses.get((topic, partition))
            if not response:
                failed_requests.append(request)
                continue

            new_stats = _Stats(response.offset, len(request.messages))
            self._update_success_topic_stats(topic, partition, new_stats)

        return failed_requests

    def _is_success_response(self, response):
        """In our case, the response is either ProduceResponse (success) or
        FailedPayloadsError (failed) if no other exception is thrown.  The
        ProduceResponse should have error == 0.
        """
        return not isinstance(response, Exception) and response.error == 0

    def _update_success_topic_stats(self, topic, partition, new_stats):
        key = _TopicPartition(topic, partition)
        self.success_topic_stats_map[key] = new_stats

        accum_stats = self.success_topic_accum_stats_map.get(key) or _Stats(0, 0)
        self.success_topic_accum_stats_map[key] = accum_stats + new_stats

    def _verify_failed_requests(self, requests, topic_offsets):
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
        failed_requests = []
        for request in requests:
            topic, partition = request.topic, request.partition
            try:
                published_count = self._get_published_msg_count(topic, topic_offsets)
                if len(request.messages) != published_count:
                    failed_requests.append(request)
                    continue

                # Update stats for the request that actually succeeds
                offset = published_count + topic_offsets[topic]
                new_stats = _Stats(offset, published_count)
                self._update_success_topic_stats(topic, partition, new_stats)

            except (error.UnknownTopic, error.UnknownPartitions):
                # May be due to the topic doesn't exist yet or stale metadata;
                # try to load the metadata for the latter case
                should_retry = self._try_load_topic_metadata(request)
                if should_retry:
                    failed_requests.append(request)

            except Exception:
                # Unable to get the high watermark of this topic; do not retry
                # this request since it's unclear if the messages are actually
                # successfully published.
                pass

        return failed_requests

    def _get_published_msg_count(self, topic, topic_offsets):
        published_msgs_count_map = get_actual_published_messages_count(
            [topic],
            topic_tracked_offset_map=topic_offsets
        )
        return published_msgs_count_map.get(topic)

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

    @property
    def total_published_message_count(self):
        return sum(stats.message_count
                   for stats in self.success_topic_accum_stats_map.values())
