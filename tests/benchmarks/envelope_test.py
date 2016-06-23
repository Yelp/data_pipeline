# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.envelope import Envelope
from tests.factories.base_factory import MessageFactory


@pytest.mark.usefixtures(
    "config_containers_connections"
)
@pytest.mark.benchmark
class TestBenchEnvelope(object):

    @pytest.fixture
    def envelope(self):
        return Envelope()

    def test_pack(self, benchmark, envelope):

        def setup():
            return [MessageFactory.create_message_with_payload_data()], {}

        benchmark.pedantic(envelope.pack, setup=setup, rounds=1000)

    def test_unpack(self, benchmark, envelope):

        def setup():
            return [envelope.pack(MessageFactory.create_message_with_payload_data())], {}

        benchmark.pedantic(envelope.unpack, setup=setup, rounds=1000)
