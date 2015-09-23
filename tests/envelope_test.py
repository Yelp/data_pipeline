# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline import message as dp_message
from data_pipeline.envelope import Envelope


class TestEnvelope(object):

    @pytest.fixture(params=[
        {'meta': None},
        {'meta': [dp_message.MetaAttribute(10, 'meta attr payload')]}
    ])
    def meta_param(self, request):
        return request.param

    @pytest.yield_fixture(autouse=True)
    def mock_meta_envelope(self, meta_param):
        unpacked_meta = meta_param['meta'][0] if meta_param['meta'] else None
        with mock.patch.object(
            Envelope,
            '_pack_meta_attribute',
            return_value={'schema_id': 10, 'payload': bytes(20)}
        ), \
            mock.patch.object(
                Envelope,
                '_unpack_meta_attribute',
                return_value=unpacked_meta
        ):
            yield

    @pytest.fixture(params=[
        (dp_message.CreateMessage, {}),
        (dp_message.RefreshMessage, {}),
        (dp_message.DeleteMessage, {}),
        (dp_message.UpdateMessage, {'previous_payload': bytes(20)})
    ])
    def message(self, request, topic_name, payload, meta_param):
        message_class, additional_params = request.param
        if meta_param:
            additional_params.update(meta_param)
        return message_class(
            topic_name,
            10,
            payload,
            **additional_params
        )

    @pytest.fixture
    def expected_message(self, message):
        previous_payload = None
        if isinstance(message, dp_message.UpdateMessage):
            previous_payload = message.previous_payload
        return dict(
            encryption_type=None,
            message_type=message.message_type.name,
            meta=message.meta,
            payload=message.payload,
            previous_payload=previous_payload,
            schema_id=message.schema_id,
            timestamp=message.timestamp,
            uuid=message.uuid
        )

    def test_pack_create_bytes(self, message, envelope):
        assert isinstance(envelope.pack(message), bytes)

    def test_pack_unpack(self, message, envelope, expected_message):
        unpacked = envelope.unpack(envelope.pack(message))
        assert unpacked == expected_message
