# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.config import get_config
from data_pipeline.position_data import PositionData


logger = get_config().logger


class PositionDataBuilder(object):
    """Makes it easier to construct and maintain PositionData.

    The basic idea is that messages are recorded as buffered, then recorded
    as published in matched pairs.  Position data can only be retrieved when
    the pairs actually match.
    """

    def __init__(self):
        self.last_published_message_position_info = None
        self.topic_to_last_position_info_map = {}
        self.topic_to_kafka_offset_map = {}
        self.unpublished_messages = 0

    def record_message_buffered(self, message):
        logger.debug("Message buffered: %s" % message)
        self.last_published_message_position_info = message.upstream_position_info
        self.topic_to_last_position_info_map[message.topic] = message.upstream_position_info
        self.unpublished_messages += 1

    def record_messages_published(self, topic, offset, message_count):
        logger.debug("Messages published: %s, %s" % (topic, message_count))
        self.topic_to_kafka_offset_map[topic] = offset + message_count
        self.unpublished_messages -= message_count

    def get_position_data(self):
        # Only allow checkpointing when there aren't unpublished messages
        assert self.unpublished_messages == 0
        return PositionData(
            last_published_message_position_info=self.last_published_message_position_info,
            topic_to_last_position_info_map=dict(self.topic_to_last_position_info_map),
            topic_to_kafka_offset_map=dict(self.topic_to_kafka_offset_map)
        )
