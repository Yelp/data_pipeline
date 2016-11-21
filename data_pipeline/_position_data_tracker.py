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

from collections import defaultdict
from collections import Mapping

from data_pipeline.config import get_config
from data_pipeline.helpers.log import debug_log
from data_pipeline.position_data import PositionData


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

    def record_message(self, message):
        """In general, `record_message_buffered` should be preferred over
        `record_message`.  `record_message` is useful for recording messages
        that will never be published, but should still be recorded.  This use
        case is important for recovery procedures, that need to record already
        published messages alongside unpublished messages, so that state
        is saved correctly.  Don't call this method unless you're positive
        you need to.
        """
        if self._should_update_position_info(message):
            self._update_position_info(message)
        self._update_merged_upstream_position_info(message)

    def update_high_watermark(self, topic, offset, message_count):
        self.topic_to_kafka_offset_map[topic] = offset + message_count

    def record_message_buffered(self, message):
        debug_log(lambda: "Message buffered: %s" % repr(message))
        self.record_message(message)
        self.unpublished_messages += 1

    def record_messages_published(self, topic, offset, message_count):
        debug_log(
            lambda: "Messages published: %s, %s" % (topic, message_count)
        )
        self.update_high_watermark(topic, offset, message_count)
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
            _update_nested_dict(
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


# TODO(joshszep|YELPLIB-65): Remove this and use function from yelp_lib
# when yelp-main makes it possible
def _update_nested_dict(original_dict, new_dict):
    """Update the dictionary and its nested dictionary fields.

    Note: This was copy-pasted from:
        opengrok/xref/submodules/yelp_lib/yelp_lib/containers/dicts.py?r=92297a46#40
        The reason is that this revision requires yelp_lib>=11.0.0 but we
        can not use this version yelp-main yet (see YELPLIB-65 for details).
        It's simpler to just temporarily pull this in.

    :param original_dict: Original dictionary
    :param new_dict: Dictionary with data to update
    """
    # Using our own stack to avoid recursion.
    stack = [(original_dict, new_dict)]
    while stack:
        original_dict, new_dict = stack.pop()
        for key, value in new_dict.items():
            if isinstance(value, Mapping):
                original_dict.setdefault(key, {})
                stack.append((original_dict[key], value))
            else:
                original_dict[key] = value
