# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import defaultdict

from yelp_lib.containers.dicts import update_nested_dict

from data_pipeline.config import get_config
from data_pipeline.position_data import PositionData


logger = get_config().logger


def PositionDataTracker():
    """Factory method for generating PositionDataTracker or subclasses
    """
    if get_config().merge_position_info_update:
        return _MergingPositionDataTracker()
    else:
        return _PositionDataTracker()


class _PositionDataTracker(object):
    """Makes it easier to construct and maintain PositionData.

    The basic idea is that messages are recorded as buffered, then recorded
    as published in matched pairs.  Position data can only be retrieved when
    the pairs actually match.
    """

    def __init__(self):
        self.unpublished_messages = 0
        self.topic_to_kafka_offset_map = {}
        self.merged_upstream_position_info_map = {}
        self._setup_position_info()

    def record_message_buffered(self, message):
        logger.debug("Message buffered: %s" % message)
        if self._should_update_position_info(message):
            self._update_position_info(message)
        self._update_merged_upstream_position_info(message)
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
            topic_to_kafka_offset_map=dict(self.topic_to_kafka_offset_map),
            merged_upstream_position_info_map=dict(self.merged_upstream_position_info_map)
        )

    def _setup_position_info(self):
        self.last_published_message_position_info = None
        self.topic_to_last_position_info_map = {}

    def _should_update_position_info(self, message):
        return (
            message.upstream_position_info is not None or
            not get_config().skip_position_info_update_when_not_set
        )

    def _update_position_info(self, message):
        self.last_published_message_position_info = message.upstream_position_info
        self.topic_to_last_position_info_map[message.topic] = message.upstream_position_info

    def _update_merged_upstream_position_info(self, message):
        if message.upstream_position_info is not None:
            update_nested_dict(
                self.merged_upstream_position_info_map,
                message.upstream_position_info
            )


class _MergingPositionDataTracker(_PositionDataTracker):
    def _setup_position_info(self):
        self.last_published_message_position_info = {}
        self.topic_to_last_position_info_map = defaultdict(dict)

    def _update_position_info(self, message):
        self.last_published_message_position_info.update(
            message.upstream_position_info
        )
        self.topic_to_last_position_info_map[message.topic].update(
            message.upstream_position_info
        )
