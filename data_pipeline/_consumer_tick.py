# -*- coding: utf-8 -*-
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
        """ Checks if it's time to tick """
        return time.time() >= self.next_tick_time
