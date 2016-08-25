# -*- coding: utf-8 -*-
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
