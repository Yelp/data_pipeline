# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline import message as dp_message


class TestEnvelope(object):

    @pytest.fixture(params=[
        {'transaction_id': None},
        {'transaction_id': 'cluster_name:log_file_name:log_position'}
    ])
    def transaction_id_params(self, request):
        return request.param

    @pytest.fixture(params=[
        (dp_message.CreateMessage, {}),
        (dp_message.RefreshMessage, {}),
        (dp_message.DeleteMessage, {}),
        (dp_message.UpdateMessage, {'previous_payload': bytes(20)})
    ])
    def message(self, request, topic_name, payload, transaction_id_params):
        message_class, additional_params = request.param
        if transaction_id_params:
            additional_params.update(transaction_id_params)
        return message_class(
            topic_name,
            10,
            payload,
            **additional_params
        )

    @pytest.fixture
    def expected_message(self, message, transaction_id_params):
        previous_payload = None
        if isinstance(message, dp_message.UpdateMessage):
            previous_payload = message.previous_payload
        return dict(
            encryption_type=None,
            message_type=message.message_type.name,
            meta=None,
            transaction_id=transaction_id_params['transaction_id'],
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
