# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.lazy_message import LazyMessage
from data_pipeline.message_type import MessageType
from tests.message_test import SharedMessageTest


class TestLazyMessage(SharedMessageTest):
    @property
    def message_class(self):
        return LazyMessage

    @property
    def valid_message_data(self):
        return dict(
            topic=str('my-topic'),
            schema_id=123,
            payload_data=dict(data='test'),
            message_type=MessageType.create
        )

    @pytest.fixture
    def valid_payload(self):
        return dict(data='test')

    def test_rejects_message_without_payload(self):
        self._assert_invalid_data(payload_data=None)

    @pytest.mark.parametrize("message_type", [
        MessageType.create, MessageType.delete, MessageType.refresh
    ])
    def test_rejects_previous_payload_unless_update(self, message_type, valid_payload):
        self._assert_invalid_data(
            previous_payload_data=valid_payload,
            message_type=message_type
        )

    def test_previous_payload_when_update(self, valid_payload):
        valid_update = self._make_message_data(
            message_type=MessageType.update,
            previous_payload_data=valid_payload
        )
        assert isinstance(self.message_class(**valid_update), self.message_class)
        self._assert_invalid_data(valid_data=valid_update, previous_payload_data=None)
        self._assert_invalid_data(valid_data=valid_update, previous_payload_data=['invalid'])
