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

from kafka_utils.util.offsets import get_topics_watermarks


def get_actual_published_messages_count(
    kafka_client,
    topics,
    topic_tracked_offset_map,
    raise_on_error=True,
):
    """Get the actual number of published messages of specified topics.

    Args:
        kafka_client (kafka.client.KafkaClient): kafka client
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
        kafka_client,
        topics,
        raise_on_error=raise_on_error
    )

    topic_to_published_msgs_count = {}
    for topic, partition_offsets in topic_watermarks.iteritems():
        high_watermark = partition_offsets[0].highmark
        offset = topic_tracked_offset_map.get(topic, 0)
        topic_to_published_msgs_count[topic] = high_watermark - offset

    return topic_to_published_msgs_count
