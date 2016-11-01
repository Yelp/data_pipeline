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


class _ConsumerTick(object):
    """ This class manages state related to ticks and triggers ticks on the
    attached Consumer every refresh_time_seconds. It can be used by
    refresh_new_topics and other methods requiring tick functionality.
    """

    def __init__(self, refresh_time_seconds):
        self.refresh_time_seconds = refresh_time_seconds
        self._reset()

    def _reset(self):
        self.next_tick_time = time.time() + self.refresh_time_seconds

    def should_tick(self):
        return time.time() >= self.next_tick_time
