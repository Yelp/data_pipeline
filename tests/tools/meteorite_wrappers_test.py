# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from data_pipeline.tools.meteorite_wrappers import StatsCounter


class TestStatsCounter(object):

    @mock.patch('yelp_meteorite.metrics.Counter.count', autospec=True)
    def test_stats_counter(self, mock_count):
        counter = StatsCounter('test_stat', message_count_timer=0, stat_type='test_type')
        counter.increment('test_type')
        assert mock_count.call_count == 1

    @mock.patch('yelp_meteorite.metrics.Counter.count', autospec=True)
    def test_batched_counter(self, mock_count):
        with mock.patch(
            'data_pipeline.tools.meteorite_wrappers.time',
        ) as mock_time:
            mock_time.time.side_effect = [
                2, 3, 8, 9
            ]
            counter = StatsCounter('test_stat', message_count_timer=4, stat_type='test_type')
            counter.increment('test_type')
            counter.increment('test_type')
            # Two increments are batched into 1 call.
            assert mock_count.call_count == 1
