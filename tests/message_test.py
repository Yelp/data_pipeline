# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.message import Message
from data_pipeline.message_type import MessageType


class SharedMessageTest(object):
    @pytest.fixture
    def message(self):
        return self.message_class(**self.valid_message_data)

    def test_rejects_unicode_topic(self):
        self._assert_invalid_data(topic=unicode('topic'))

    def test_rejects_empty_topic(self):
        self._assert_invalid_data(topic=str(''))

    def test_rejects_nonnumeric_schema_id(self):
        self._assert_invalid_data(schema_id='123')

    def test_rejects_junk_uuid(self):
        self._assert_invalid_data(uuid='junk')

    def test_rejects_pii_data(self):
        self._assert_invalid_data(NotImplementedError, contains_pii=True)

    def _assert_invalid_data(self, error=ValueError, valid_data=None, **data_overrides):
        invalid_message_data = self._make_message_data(valid_data, **data_overrides)
        with pytest.raises(error):
            self.message_class(**invalid_message_data)

    def _make_message_data(self, valid_data=None, **overrides):
        if valid_data is None:
            valid_data = self.valid_message_data
        message_data = dict(valid_data)
        message_data.update(**overrides)
        return message_data

    def test_generates_uuid(self, message):
        assert isinstance(message.uuid, bytes) and len(message.uuid) == 16

    def test_accepts_only_dicts_in_upstream_position_info(self):
        valid_update = self._make_message_data(
            upstream_position_info=dict(something='some_unicode')
        )
        assert isinstance(self.message_class(**valid_update), self.message_class)
        self._assert_invalid_data(upstream_position_info='test')
        self._assert_invalid_data(upstream_position_info=['test'])


class TestMessage(SharedMessageTest):
    @property
    def message_class(self):
        return Message

    @property
    def valid_message_data(self):
        return dict(
            topic=str('my-topic'),
            schema_id=123,
            payload=bytes(10),
            message_type=MessageType.create
        )

    def test_rejects_message_without_payload(self):
        self._assert_invalid_data(payload='')

    @pytest.mark.parametrize("message_type", [
        MessageType.create, MessageType.delete, MessageType.refresh
    ])
    def test_rejects_previous_payload_unless_update(self, message_type):
        self._assert_invalid_data(
            previous_payload=bytes(10),
            message_type=message_type
        )

    def test_previous_payload_when_update(self):
        valid_update = self._make_message_data(
            message_type=MessageType.update,
            previous_payload=bytes(100)
        )
        assert isinstance(self.message_class(**valid_update), self.message_class)
        self._assert_invalid_data(valid_data=valid_update, previous_payload=None)
        self._assert_invalid_data(valid_data=valid_update, previous_payload="")
