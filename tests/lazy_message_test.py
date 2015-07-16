# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline import lazy_message as dp_lazy_message
from data_pipeline.message import InvalidOperation
from data_pipeline.message_type import MessageType
from tests.message_test import SharedMessageTest


class PayloadOnlyLazyMessageTest(SharedMessageTest):

    @property
    def valid_payload_data(self):
        return {'data': 'test'}

    @property
    def valid_message_data(self):
        return {
            'topic': str('my-topic'),
            'schema_id': 123,
            'payload_data': self.valid_payload_data,
        }

    def test_rejects_message_without_payload_data(self):
        self._assert_invalid_data(payload_data=None)

    def test_rejects_previous_payload(self, message):
        with pytest.raises(InvalidOperation):
            message.previous_payload
        with pytest.raises(InvalidOperation):
            message.previous_payload = bytes(10)

    def test_rejects_previous_payload_data(self, message):
        with pytest.raises(InvalidOperation):
            message.previous_payload_data
        with pytest.raises(InvalidOperation):
            message.previous_payload_data = {'data': 'test'}

    def test_dry_run(self):
        dry_run_message = self.message_class(dry_run=True, **self.valid_message_data)
        assert dry_run_message.payload == repr(self.valid_payload_data)


class TestLazyCreateMessage(PayloadOnlyLazyMessageTest):

    @property
    def message_class(self):
        return dp_lazy_message.LazyCreateMessage

    @property
    def expected_message_type(self):
        return MessageType.create


class TestLazyRefreshMessage(PayloadOnlyLazyMessageTest):

    @property
    def message_class(self):
        return dp_lazy_message.LazyRefreshMessage

    @property
    def expected_message_type(self):
        return MessageType.refresh


class TestLazyDeleteMessage(PayloadOnlyLazyMessageTest):

    @property
    def message_class(self):
        return dp_lazy_message.LazyDeleteMessage

    @property
    def expected_message_type(self):
        return MessageType.delete


class TestLazyUpdateMessage(SharedMessageTest):

    @property
    def message_class(self):
        return dp_lazy_message.LazyUpdateMessage

    @property
    def expected_message_type(self):
        return MessageType.update

    @property
    def valid_payload_data(self):
        return {'data': 'test'}

    @property
    def valid_previous_payload_data(self):
        return {'previous_data': 'foo'}

    @property
    def valid_message_data(self):
        return dict(
            topic=str('my-topic'),
            schema_id=123,
            payload_data=self.valid_payload_data,
            previous_payload_data=self.valid_previous_payload_data
        )

    def test_rejects_message_without_payload_data(self):
        self._assert_invalid_data(payload_data=None)

    def test_rejects_message_without_previous_payload_data(self, message):
        self._assert_invalid_data(previous_payload_data=None)
        self._assert_invalid_data(previous_payload_data="")

    def test_dry_run(self):
        dry_run_message = self.message_class(dry_run=True, **self.valid_message_data)
        assert dry_run_message.payload == repr(self.valid_payload_data)
        assert dry_run_message.previous_payload == repr(self.valid_previous_payload_data)
