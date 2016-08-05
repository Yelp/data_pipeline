# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from datetime import timedelta

import pysensu_yelp

from data_pipeline.tools.heartbeat_periodic_processor import HeartbeatPeriodicProcessor


class SensuAlertManager(HeartbeatPeriodicProcessor):
    """ This class triggers sensu alert if the producer falls behind real time.
    The check will be triggered every 30 seconds to avoid flapping and overflowing
    sensu servers. And if we fall behind and then recover, sensu will take care of
    resolving this alert itself.

    inputs:
    interval_in_seconds -- int check_every interval for sensu
    result_dict --  dictionary with sensu parameters, for example:
        result_dict = {
            'name': 'replication_handler_real_time_check',
            'output': 'Replication Handler has caught up with real time.',
            'runbook': 'y/datapipeline',
            'status': 0,
            'team': 'bam',
            'page': False,
            'notification_email': 'bam+sensu@yelp.com',
            'check_every': '{time}s'.format(time=self.interval_in_seconds),
            'alert_after': '5m',
            'ttl': '300s',
            'sensu_host': config.env_config.sensu_host,
            'source': config.env_config.sensu_source,
        }
    service_name -- string (possibly with underscores)
    disable -- boolean to disable sensu
    max_delay_minutes -- integer number of minutes before timing out
    """

    def __init__(
        self,
        interval_in_seconds,
        service_name,
        result_dict,
        max_delay_minutes,
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
        self._max_delay_allowed_in_minutes = max_delay_minutes

    def process(self, timestamp):
        if self.disable:
            return

        # This timestamp param has to be timezone aware, otherwise it will not be
        # able to compare with timezone aware timestamps.
        delay_time = self._utc_now - timestamp
        if delay_time > timedelta(minutes=self._max_delay_allowed_in_minutes):
            self._result_dict.update({
                'status': 2,
                'output': '{service} is falling {delay_time} min behind real time'.format(
                    service=self._service_name,
                    delay_time=delay_time),
            })
        self._log.info("{} status: {}, output: {}.".format(
            self._service_name,
            self._result_dict['status'],
            self._result_dict['output']))

        self.send_event()

    def send_event(self):
        pysensu_yelp.send_event(**self._result_dict)
