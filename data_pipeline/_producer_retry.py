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
"""
This module contains classes that implement retry logic that provides various
publish guarantees.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from kafka.common import LeaderNotAvailableError

from data_pipeline._kafka_util import get_actual_published_messages_count
from data_pipeline.config import get_config
from data_pipeline.publish_guarantee import PublishGuaranteeEnum


logger = get_config().logger


_TopicPartition = namedtuple('_TopicPartition', ['topic_name', 'partition'])


_Stats = namedtuple('_Stats', ['original_offset', 'message_count'])


class RetryHandler(object):
    """The class tracks the message publishing statistics in each retry,
    such as topic offset, number of published messages, etc., and determines
    which messages should be retried based on specified publishing guarantee.
    """

    # The logging is explicitly added in the class instead of being delegated to
    # `LoggingKafkaProducer` to help tracking issues when they occur.

    def __init__(
        self,
        kafka_client,
        requests,
        publish_guarantee=PublishGuaranteeEnum.exact_once,
    ):
        self.kafka_client = kafka_client
        self.initial_requests = requests
        self.requests_to_be_sent = requests
        self.publish_guarantee = publish_guarantee
        self.success_topic_stats_map = {}
        self.success_topic_accum_stats_map = {}

    def update_requests_to_be_sent(self, responses, topic_offsets=None):
        """Update stats from the responses of the publishing requests and
        determine which messages should be retried.

        Args:
            responses (kafka.common.FetchResponse or kafka.common.KafkaError):
                responses of the requests that publish messages to kafka topics
            topic_offsets (Optional[dict]): offset of each topic tracked by the
                producer so far.  It is used for exact-once publishing guarantee.
        """
        self.success_topic_stats_map = {}
        requests_to_retry = self._update_success_requests_stats(
            self.requests_to_be_sent,
            responses
        )
        if self.publish_guarantee == PublishGuaranteeEnum.exact_once:
            requests_to_retry = self._verify_failed_requests(
                requests_to_retry,
                topic_offsets
            )
        self.requests_to_be_sent = requests_to_retry

    def _update_success_requests_stats(self, requests, responses):
        """Update publish stats of successful requests and return the list of
        requests that do not have success responses and need to be retried.
        """
        success_responses = {
            (r.topic, r.partition): r
            for r in responses if self._is_success_response(r)
        }

        requests_to_retry = []
        for request in requests:
            topic, partition = request.topic, request.partition

            response = success_responses.get((topic, partition))
            if not response:
                requests_to_retry.append(request)
                continue

            new_stats = _Stats(response.offset, len(request.messages))
            self._update_success_topic_stats(topic, partition, new_stats)

        return requests_to_retry

    def _is_success_response(self, response):
        """In our case, the response is either ProduceResponse (success) or
        FailedPayloadsError (failed) if no other exception is thrown.  The
        ProduceResponse should have error == 0.
        """
        return not isinstance(response, Exception) and response.error == 0

    def _update_success_topic_stats(self, topic, partition, new_stats):
        key = _TopicPartition(topic, partition)
        self.success_topic_stats_map[key] = new_stats
        self.success_topic_accum_stats_map[key] = new_stats

    def _verify_failed_requests(self, requests, topic_offsets):
        """Verify if the requests actually fail by checking the high watermark
        of the corresponding topics.  If the high watermark of a topic matches
        the number of messages in the request, the request is considered as
        successfully published, and the offset is saved in the position_data_tracker.

        If the high watermark data cannot be retrieved and it is not due to
        missing topic/partition, the request will be considered as failed but
        won't be retried because it cannot determine whether the messages are
        actually published.  Otherwise, the request will be retried.
        """
        # `get_topics_watermarks` fails all the topics if any partition leader
        # is not available, so here it checks each topic individually.
        requests_to_retry = []
        for request in requests:
            topic, partition = request.topic, request.partition
            topic_desc = "topic {} partition {} request".format(topic, partition)
            try:
                logger.debug("Verifying failed {}.".format(topic_desc))

                # try to load the metadata in case it is stale
                success = self._try_load_topic_metadata(topic)
                if not success:
                    logger.debug("Cannot load the metadata of topic {}. Skip {}."
                                 .format(topic, topic_desc))
                    continue

                published_count = self._get_published_msg_count(topic, topic_offsets)
                if len(request.messages) != published_count:
                    logger.debug(
                        "Request message count {} doesn't match actual published "
                        "message count {}. Retry {}.".format(
                            len(request.messages),
                            published_count,
                            topic_desc
                        )
                    )
                    requests_to_retry.append(request)
                    continue

                # Update stats for the request that actually succeeds
                logger.debug("{} actually succeeded.".format(topic_desc))
                new_stats = _Stats(topic_offsets[topic], published_count)
                self._update_success_topic_stats(topic, partition, new_stats)

            except LeaderNotAvailableError:
                # Topic doesn't exist yet but the broker is configured to create
                # the topic automatically. Retry the request.
                logger.debug(
                    "Topic {} doesn't exists. Retry {}.".format(topic, topic_desc)
                )
                requests_to_retry.append(request)

            except Exception:
                # Unable to get the high watermark of this topic; do not retry
                # this request since it's unclear if the messages are actually
                # successfully published.
                logger.debug(
                    "Cannot get the high watermark. Skip {}.".format(topic_desc),
                    exc_info=1
                )

        return requests_to_retry

    def _get_published_msg_count(self, topic, topic_offsets):
        published_msgs_count_map = get_actual_published_messages_count(
            self.kafka_client,
            [topic],
            topic_tracked_offset_map=topic_offsets
        )
        return published_msgs_count_map.get(topic)

    def _try_load_topic_metadata(self, topic):
        """Try to load the metadata of the topic of the given request.  It
        returns True if it succeeds, and False otherwise with one exception:
        if the topic doesn't exist but the broker is configured to create it
        automatically, it will re-raise LeaderNotAvailableError.
        """
        try:
            self.kafka_client.load_metadata_for_topics(topic)
            return True
        except LeaderNotAvailableError:
            raise
        except Exception:
            logger.exception("Failed to load metadata of topic {}.".format(topic))
            return False

    @property
    def total_published_message_count(self):
        return sum(stats.message_count
                   for stats in self.success_topic_accum_stats_map.values())

    @property
    def has_unpublished_request(self):
        """Whether any request from the initial publishing requests list hasn't
        been successfully sent."""
        request_topics_partitions = {
            (r.topic, r.partition) for r in self.initial_requests
        }
        response_topics_partitions = {
            (key.topic_name, key.partition)
            for key in self.success_topic_accum_stats_map.keys()
        }
        return not request_topics_partitions.issubset(response_topics_partitions)
