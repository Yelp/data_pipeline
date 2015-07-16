# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline import message as dp_message
from data_pipeline.message_type import MessageType


class SharedMessageTest(object):

    @pytest.fixture
    def message(self):
        return self.message_class(**self.valid_message_data)

    def test_rejects_unicode_topic(self):
        self._assert_invalid_data(topic=unicode('topic'))

    def test_rejects_empty_topic(self):
        self._assert_invalid_data(topic=str(''))

    def test_rejects_non_numeric_schema_id(self):
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

    def test_message_type(self, message):
        assert message.message_type == self.expected_message_type


class PayloadOnlyMessageTest(SharedMessageTest):

    @property
    def valid_message_data(self):
        return {
            'topic': str('my-topic'),
            'schema_id': 123,
            'payload': bytes(10),
        }

    def test_rejects_message_without_payload(self):
        self._assert_invalid_data(payload='')

    def test_rejects_previous_payload(self, message):
        with pytest.raises(dp_message.InvalidOperation):
            message.previous_payload
        with pytest.raises(dp_message.InvalidOperation):
            message.previous_payload = bytes(10)

    def test_rejects_previous_payload_data(self, message):
        with pytest.raises(dp_message.InvalidOperation):
            message.previous_payload_data
        with pytest.raises(dp_message.InvalidOperation):
            message.previous_payload_data = {'data': 'test'}


class TestCreateMessage(PayloadOnlyMessageTest):

    @property
    def message_class(self):
        return dp_message.CreateMessage

    @property
    def expected_message_type(self):
        return MessageType.create


class TestRefreshMessage(PayloadOnlyMessageTest):

    @property
    def message_class(self):
        return dp_message.RefreshMessage

    @property
    def expected_message_type(self):
        return MessageType.refresh


class TestDeleteMessage(PayloadOnlyMessageTest):

    @property
    def message_class(self):
        return dp_message.DeleteMessage

    @property
    def expected_message_type(self):
        return MessageType.delete


class TestUpdateMessage(SharedMessageTest):

    @property
    def message_class(self):
        return dp_message.UpdateMessage

    @property
    def expected_message_type(self):
        return MessageType.update

    @property
    def valid_message_data(self):
        return dict(
            topic=str('my-topic'),
            schema_id=123,
            payload=bytes(10),
            previous_payload=bytes(100)
        )

    def test_rejects_message_without_payload(self):
        self._assert_invalid_data(payload='')

    def test_rejects_message_without_previous_payload(self, message):
        self._assert_invalid_data(previous_payload=None)
        self._assert_invalid_data(previous_payload="")
