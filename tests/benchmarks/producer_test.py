# -*- coding: utf-8 -*-
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
    # "patch_monitor_init_start_time_to_now",
    "containers"
)
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

    # this test needs to be fixed
    # def test_publish(self, benchmark, dp_producer):

    #     def setup():
    #         return [MessageFactory.create_message_with_payload_data()], {}
    #     # timeout for flush currently is 100ms, on an average msg publish take ~1ms
    #     # 10000 round be ensure 100 flushes
    #     benchmark.pedantic(dp_producer.publish, setup=setup, rounds=10000)
