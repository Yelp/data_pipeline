# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

from data_pipeline.config import get_config


class ConsumerTick(object):
    """Ticks are controlled by the window_max_time_seconds config property.
    This class manages state related to ticks and triggers ticks on the
    attached Consumer when appropriate. This class will accept a manual
    override kwarg as well; that way it can be used by refresh_new_topics and
    other methods requiring tick functionality.
    """

    def __init__(self, refresh_time_seconds=None):
        self.refresh_time_seconds = refresh_time_seconds or get_config().window_max_time_seconds
        self._reset()

    def _reset(self):
        self.next_tick_time = time.time() + self.refresh_time_seconds

    def should_tick(self):
        """Will trigger a tick on the Consumer if necessary"""
        return time.time() >= self.next_tick_time
