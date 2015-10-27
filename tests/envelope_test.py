# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline import message as dp_message
from data_pipeline.meta_attribute import MetaAttribute


class TestEnvelope(object):

    @pytest.fixture(params=[
        None,
        {'good_payload': 26}
    ])
    def meta_attr_payload(self, request):
        return request.param

    @pytest.fixture
    def valid_meta(self, meta_attr_payload, registered_meta_attribute):
        if meta_attr_payload is None:
            return None
        meta_attr = MetaAttribute()
        meta_attr.schema_id = registered_meta_attribute.schema_id
        meta_attr.payload = meta_attr_payload
        return [meta_attr]

    @pytest.fixture
    def meta_attr_param(self, valid_meta):
        return {'meta': valid_meta}

    @pytest.fixture(params=[
        (dp_message.CreateMessage, {}),
        (dp_message.RefreshMessage, {}),
        (dp_message.DeleteMessage, {}),
        (dp_message.UpdateMessage, {'previous_payload': bytes(20)})
    ])
    def message(self, request, topic_name, payload, meta_attr_param):
        message_class, additional_params = request.param
        if meta_attr_param:
            additional_params.update(meta_attr_param)
        return message_class(
            schema_id=10,
            topic=topic_name,
            payload=payload,
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
            meta=[
                meta_attr.avro_repr
                for meta_attr in message.meta
            ] if message.meta else None,
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
