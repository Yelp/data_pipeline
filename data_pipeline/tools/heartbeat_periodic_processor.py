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

from dateutil.tz import tzutc


class BasePeriodicProcessor(object):
    """ This class provides an interface for handling periodic events that can
    be triggered by a heartbeat event, like sensu alert and data event checkpoint.
    That is, it serves as a base class and must be subclassed.

    Args:
      interval_in_seconds(int): the time interval between two events.
    """

    def __init__(self, interval_in_seconds):
        self.interval_in_seconds = interval_in_seconds
        self._next_process_time = self._utc_now

    def periodic_process(self, timestamp=None):
        """ This method remains because it's called by the replication handler;
        if / when we start calling the process method below directly from the
        replication handler we can remove it (DATAPIPE-1435)
        Args:
            timestamp(datetime.datetime): the datetime of the event with utc
        """
        if self._should_process():
            self.process(timestamp)
            self._next_process_time = self._compute_next_process_time()

    def process(self, timestamp=None):
        raise NotImplementedError

    def _should_process(self):
        return self._utc_now >= self._next_process_time

    def _compute_next_process_time(self):
        return self._utc_now + timedelta(seconds=self.interval_in_seconds)

    @property
    def _utc_now(self):
        return datetime.now(tzutc())
