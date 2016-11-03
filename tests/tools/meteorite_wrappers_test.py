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
