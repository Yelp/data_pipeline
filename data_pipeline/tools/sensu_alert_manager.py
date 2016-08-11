# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from datetime import timedelta

import pysensu_yelp

from data_pipeline.tools.heartbeat_periodic_processor import HeartbeatPeriodicProcessor

SENSU_DELAY_ALERT_INTERVAL_SECONDS = 30


class SensuAlertManager(HeartbeatPeriodicProcessor):
    """ This class triggers sensu alert if the producer falls behind real time.
    The check will be triggered every 30 seconds to avoid flapping and overflowing
    sensu servers. And if we fall behind and then recover, sensu will take care of
    resolving this alert itself.

    Args:
        interval_in_seconds(int): check_every interval for sensu
        result_dict(dict): dictionary with sensu parameters.  For details see
             http://pysensu-yelp.readthedocs.io/en/latest/index.html?highlight=send_event for details
        service_name(str): name of the service possibly with underscores
        disable(bool): disable this alert manager
        max_delay_seconds(int): number of seconds before timing out
    """

    def __init__(
        self,
        interval_in_seconds,
        service_name,
        result_dict,
        max_delay_seconds,
        disable=False
    ):
        super(SensuAlertManager, self).__init__(interval_in_seconds)
        self._result_dict = result_dict
        self._result_dict['check_every'] = result_dict.get(
            'check_every',
            '{time}s'.format(time=self.interval_in_seconds)
        )
        self._service_name = service_name
        self._log = logging.getLogger('{}.util.sensu_alert_manager'.format(service_name))
        self.disable = disable
        self._max_delay_allowed_in_seconds = max_delay_seconds

    def process(self, timestamp):
        if timestamp is None or self.disable:
            return

        # This timestamp param has to be timezone aware, otherwise it will not be
        # able to compare with timezone aware timestamps.
        delay_time = self._utc_now - timestamp
        if delay_time > timedelta(seconds=self._max_delay_allowed_in_seconds):
            self._result_dict.update({
                'status': 2,
                'output': '{service} is falling {delay_time} min behind real time'.format(
                    service=self._service_name,
                    delay_time=delay_time),
            })
        self._log.info("{} status: {}, output: {}.".format(
            self._service_name,
            self._result_dict['status'],
            self._result_dict['output'])
        )

        self.send_event()

    def send_event(self):
        pysensu_yelp.send_event(**self._result_dict)
