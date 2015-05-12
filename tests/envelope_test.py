# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.message import Message
from data_pipeline.message_type import MessageType


class TestEnvelope(object):
    @pytest.fixture(params=[
        (MessageType.create, None),
        (MessageType.refresh, None),
        (MessageType.delete, None),
        (MessageType.update, bytes(20))
    ])
    def message(self, request, topic_name, payload):
        message_type, previous_payload = request.param
        return Message(
            topic_name,
            10,
            payload,
            message_type,
            previous_payload=previous_payload
        )

    def test_pack_create_bytes(self, message, envelope):
        assert isinstance(envelope.pack(message), bytes)

    def test_pack_unpack(self, message, envelope):
        unpacked = envelope.unpack(envelope.pack(message))
        expected_message = dict(
            encryption_type=None,
            message_type=message.message_type.name,
            meta=None,
            payload=message.payload,
            previous_payload=message.previous_payload,
            schema_id=message.schema_id,
            timestamp=message.timestamp,
            uuid=message.uuid
        )
        assert unpacked == expected_message
