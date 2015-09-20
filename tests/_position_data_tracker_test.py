# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import defaultdict

import pytest

from data_pipeline._position_data_tracker import PositionDataTracker
from data_pipeline.message import CreateMessage
from tests.helpers.config import reconfigure


class BasePositionDataTrackerTest(object):
    @property
    def valid_message_data(self):
        return {
            'topic': self.topic,
            'schema_id': 123,
            'payload': bytes(10)
        }

    @property
    def topic(self):
        return str('my-topic')

    def _publish_messages(self, tracker, messages):
        messages_published = defaultdict(int)
        for message in messages:
            tracker.record_message_buffered(message)
            messages_published[message.topic] += 1

        for topic, count in messages_published.iteritems():
            tracker.record_messages_published(topic, 0, count)

    def _create_message(self, **kwargs):
        message_data = self.valid_message_data
        message_data.update(kwargs)
        return CreateMessage(**message_data)


class TestPositionDataTracker(BasePositionDataTrackerTest):
    @pytest.fixture
    def tracker(self):
        return PositionDataTracker()

    @pytest.fixture
    def position_info(self):
        return {0: 10}

    def test_publishing_message_sets_position_info(self, tracker, position_info):
        self._publish_messages(tracker, [
            self._create_message(upstream_position_info=position_info),
        ])
        position_data = tracker.get_position_data()
        assert position_data.last_published_message_position_info == position_info
        assert position_data.topic_to_last_position_info_map == {self.topic: position_info}

    def test_publishing_message_without_position_info_clears_position_info(self, tracker):
        self._publish_messages(tracker, [
            self._create_message(upstream_position_info={0: 10}),
            self._create_message(upstream_position_info=None)
        ])
        position_data = tracker.get_position_data()
        assert position_data.last_published_message_position_info is None
        assert position_data.topic_to_last_position_info_map == {self.topic: None}

    def test_publishing_message_when_skipping_unset_position_info(self, tracker, position_info):
        with reconfigure(skip_position_info_update_when_not_set=True):
            self._publish_messages(tracker, [
                self._create_message(upstream_position_info=position_info),
                self._create_message(upstream_position_info=None)
            ])
            position_data = tracker.get_position_data()
        assert position_data.last_published_message_position_info == position_info
        assert position_data.topic_to_last_position_info_map == {self.topic: position_info}

    def test_publishing_message_with_different_position_info_keys(self, tracker):
        self._publish_messages(tracker, [
            self._create_message(upstream_position_info={0: 10}),
            self._create_message(upstream_position_info={1: 12})
        ])
        position_data = tracker.get_position_data()
        assert position_data.last_published_message_position_info == {1: 12}
        assert position_data.topic_to_last_position_info_map == {self.topic: {1: 12}}


class TestMergingPositionDataTracker(BasePositionDataTrackerTest):
    @pytest.yield_fixture
    def tracker(self):
        with reconfigure(
            skip_position_info_update_when_not_set=True,
            merge_position_info_update=True
        ):
            yield PositionDataTracker()

    def test_publishing_with_merged_position_info(self, tracker):
        self._publish_messages(tracker, [
            self._create_message(upstream_position_info={0: 10}),
            self._create_message(upstream_position_info=None),
            self._create_message(upstream_position_info={0: 12, 1: 14}),
            self._create_message(upstream_position_info={1: 18, 2: 20}),
        ])
        position_data = tracker.get_position_data()
        expected_position_info = {0: 12, 1: 18, 2: 20}
        assert position_data.last_published_message_position_info == expected_position_info
        assert position_data.topic_to_last_position_info_map == {self.topic: expected_position_info}
