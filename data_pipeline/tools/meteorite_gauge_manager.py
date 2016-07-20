# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.heartbeat_periodic_processor import HeartbeatPeriodicProcessor
from data_pipeline.tools.meteorite_wrappers import StatGauge


class MeteoriteGaugeManager(HeartbeatPeriodicProcessor):
    """
    This class reports how far behind real-time the producer is to meteorite/signalfx

    Args:
       interval_in_seconds(int): the time interval between two events.
       stats_gauge_name(str): name of the stats gauge
       container_name(str): paasta container name
       container_env(str): paasta cluster name
       disable(bool): whether this gauge is disabled or not
       kwargs(dict): any additional keyword args
    """

    def __init__(
        self,
        interval_in_seconds,
        stats_gauge_name=None,
        container_name=None,
        container_env=None,
        disable=False,
        **kwargs
    ):
        super(MeteoriteGaugeManager, self).__init__(interval_in_seconds)
        self.gauge = StatGauge(
            stats_gauge_name,
            container_name=container_name,
            container_env=container_env,
            **kwargs
        )
        self.disable = disable

    def process(self, timestamp):
        if self.disable:
            return

        delay_seconds = (self._utc_now - timestamp).total_seconds()
        self.gauge.set(delay_seconds)
