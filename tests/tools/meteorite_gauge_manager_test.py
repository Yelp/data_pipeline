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

from datetime import datetime
from datetime import timedelta

import mock
from dateutil.tz import tzutc

from data_pipeline.tools.meteorite_gauge_manager import MeteoriteGaugeManager


class TestMeteoriteGaugeManager(object):

    @mock.patch('yelp_meteorite.metrics.Gauge.set', autospec=True)
    def test_gauge_manager_call_count(self, mock_set):
        gauge = MeteoriteGaugeManager(
            interval_in_seconds=10,
            stats_gauge_name='test_gauge'
        )
        ts = datetime.now(tzutc())
        gauge.process(ts)
        assert mock_set.call_count == 1

    @mock.patch('yelp_meteorite.metrics.Gauge.set', autospec=True)
    def test_gauge_manager_process_args(self, mock_set):
        with mock.patch(
            'data_pipeline.tools.meteorite_gauge_manager.MeteoriteGaugeManager._utc_now',
            new_callable=mock.PropertyMock
        ) as utc_now:
            fake_time = datetime(year=2016, month=1, day=1)
            utc_now.return_value = fake_time
            gauge = MeteoriteGaugeManager(
                interval_in_seconds=10,
                stats_gauge_name='test_gauge'
            )
            ts = fake_time - timedelta(seconds=60)
            gauge.process(ts)
            assert mock_set.call_args[0][1] == 60.0

    @mock.patch('yelp_meteorite.metrics.Gauge.set', autospec=True)
    def test_gauge_manager_disabled(self, mock_set):
        gauge = MeteoriteGaugeManager(
            interval_in_seconds=10,
            stats_gauge_name='test_gauge',
            disable=True
        )
        ts = datetime.now(tzutc())
        gauge.process(ts)
        assert mock_set.call_count == 0
