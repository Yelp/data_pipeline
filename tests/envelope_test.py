# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class TestEnvelope(object):
    def test_pack_create_bytes(self, message, envelope):
        assert isinstance(envelope.pack(message), bytes)

    def test_pack_unpack(self, message, envelope):
        unpacked = envelope.unpack(envelope.pack(message))
        message_repr = message.get_avro_repr()
        message_repr.update(dict(
            encryption_type=None,
            meta=None,
            previous_payload=None
        ))
        assert unpacked == message_repr
