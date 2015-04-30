from __future__ import absolute_import
import pytest

from data_pipeline.message import Message
from data_pipeline.message_type import MessageType


class TestMessage(object):
    @property
    def valid_message_data(self):
        return dict(
            topic='my-topic',
            schema_id=123,
            payload=bytes(10),
            message_type=MessageType.create
        )

    @pytest.fixture
    def message(self):
        return Message(**self.valid_message_data)

    def test_rejects_unicode_topic(self):
        self._assert_invalid_data(topic=unicode('topic'))

    def test_rejects_empty_topic(self):
        self._assert_invalid_data(topic=str(''))

    def test_rejects_nonnumeric_schema_id(self):
        self._assert_invalid_data(schema_id='123')

    def test_rejects_message_without_payload(self):
        self._assert_invalid_data(payload='')

    def test_rejects_junk_uuid(self):
        self._assert_invalid_data(uuid='junk')

    def test_rejects_pii_data(self):
        self._assert_invalid_data(NotImplementedError, contains_pii=True)

    def test_generates_uuid(self, message):
        assert isinstance(message.uuid, bytes) and len(message.uuid) == 16

    def test_message_can_be_packed(self, message, envelope):
        assert isinstance(envelope.pack(message), bytes)

    def _assert_invalid_data(self, error=ValueError, **data_overrides):
        invalid_message_data = self._make_message_data(**data_overrides)
        with pytest.raises(error):
            Message(**invalid_message_data)

    def _make_message_data(self, **overrides):
        message_data = dict(self.valid_message_data)
        message_data.update(**overrides)
        return message_data
