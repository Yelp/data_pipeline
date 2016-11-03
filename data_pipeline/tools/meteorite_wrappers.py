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
from collections import defaultdict

import yelp_meteorite


class StatsBase(object):
    """ This class simply guarantees the other Stats classes will have a process method"""

    def process(self, topic):
        raise NotImplementedError


class StatsCounter(StatsBase):
    """ This class provides an easy way to send batched multi-dimension stats to meteorite/signalFX.
    Args:
      stats_counter_name(str): the name of this stat.
      message_counte_timer(float): the time interval between batch flushes.
      kwargs(dict): the stat dimensions
    """

    def __init__(self, stat_counter_name, message_count_timer=0.25, **kwargs):
        self.dimensions = kwargs

        self.message_count_timer = message_count_timer
        self._meteorite_counter = yelp_meteorite.create_counter(
            stat_counter_name,
            self.dimensions
        )
        self._reset()

    def _reset(self):
        self.counts = defaultdict(int)
        self.flush_time = time.time() + self.message_count_timer

    def increment(self, topic):
        """Increments the counter for the given topic"""
        self.counts[topic] += 1
        self.wake()

    def wake(self):
        """Allows the counter to flush to meteorite if necessary"""
        time_now = time.time()
        if time_now >= self.flush_time:
            self.flush()

    def flush(self):
        """Causes all of the existing counts to be sent to meteorite"""
        for topic, count in self.counts.iteritems():
            self._meteorite_counter.count(count, {'topic': topic})
        self._reset()

    def process(self, topic):
        """increments the counter for the topic specified

        Args:
            topic(str): the name of the topic to which a message is published
        """
        self.increment(topic)


class StatTimer(StatsBase):
    """This class exists primarily because we currently do not know if any sort
    of batching and/or sampling mechanisms will be necessary to avoid data loss
    in meteorite.
    """

    def __init__(self, stat_timer_name, **kwargs):
        self.dimensions = kwargs

        self._meteorite_timer = yelp_meteorite.create_timer(
            stat_timer_name,
            self.dimensions
        )

    def start(self):
        self._meteorite_timer.start()

    def stop(self, tmp_dimensions=None):
        self._meteorite_timer.stop(tmp_dimensions)


class StatGauge(StatsBase):
    """This class exists for the same reason as StatTimer."""

    def __init__(self, stat_gauge_name, **kwargs):
        self.dimensions = kwargs

        self._meteorite_gauge = yelp_meteorite.create_gauge(
            stat_gauge_name,
            self.dimensions
        )

    def set(self, value, tmp_dimensions=None):
        self._meteorite_gauge.set(value, tmp_dimensions)

    def process(self, value, tmp_dimensions):
        """sets the value of a gauge in meteorite

        Args:
            value(int): the value for the dimensions specified
            tmp_dimensions(dict): the dimensions for each emission
        """
        self.set(value, tmp_dimensions)
