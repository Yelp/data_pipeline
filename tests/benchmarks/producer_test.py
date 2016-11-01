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

import time

import mock
import pytest

from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.producer import Producer
from tests.factories.base_factory import MessageFactory


@pytest.mark.usefixtures(
    "configure_teams",
    "config_containers_connections"
)
@pytest.mark.benchmark
class TestBenchProducer(object):

    @pytest.yield_fixture
    def patch_monitor_init_start_time_to_now(self):
        with mock.patch(
            'data_pipeline.client._Monitor.get_monitor_window_start_timestamp',
            return_value=int(time.time())
        ) as patched_start_time:
            yield patched_start_time

    @pytest.yield_fixture
    def dp_producer(self, team_name):
        with Producer(
            producer_name='producer_1',
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False
        ) as producer:
            yield producer

    def test_publish(self, benchmark, dp_producer):

        def setup():
            return [MessageFactory.create_message_with_payload_data()], {}

        # Publishing a message takes 1ms on average.
        # Messages are flushed every 100ms.
        # config::kafka_producer_flush_time_limit_seconds
        #
        # Perform 2000 rounds to ensure 20 flushes.
        benchmark.pedantic(dp_producer.publish, setup=setup, rounds=2000)
