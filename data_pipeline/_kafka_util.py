# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from yelp_kafka.offsets import get_topics_watermarks

from data_pipeline.config import get_config


def get_actual_published_messages_count(
    topics,
    topic_tracked_offset_map,
    raise_on_error=True,
    kafka_client=None
):
    """Get the actual number of published messages of specified topics.

    Args:
        topics ([str]): List of topic names to get message count
        topic_tracked_offset_map (dict(str, int)): dictionary which
            contains each topic and its current stored offset value.
        raise_on_error (Optional[bool]): if False,  the function ignores
            missing topics and missing partitions. It still may fail on
            the request send.  Default to True.
        kafka_client (Optional[kafka.client.KafkaClient]): kafka client

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
    _kafka_client = kafka_client or get_config().kafka_client
    topic_watermarks = get_topics_watermarks(
        _kafka_client,
        topics,
        raise_on_error=raise_on_error
    )

    topic_to_published_msgs_count = {}
    for topic, partition_offsets in topic_watermarks.iteritems():
        high_watermark = partition_offsets[0].highmark
        offset = topic_tracked_offset_map.get(topic, 0)
        topic_to_published_msgs_count[topic] = high_watermark - offset

    return topic_to_published_msgs_count
