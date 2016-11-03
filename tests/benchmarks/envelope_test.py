# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
