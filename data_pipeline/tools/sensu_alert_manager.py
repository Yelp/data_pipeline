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

import copy
import logging
from datetime import timedelta

import pysensu_yelp

from data_pipeline.tools.heartbeat_periodic_processor import BasePeriodicProcessor


class SensuAlertManager(BasePeriodicProcessor):
    """ This class triggers sensu alert if the producer falls behind real time.
    The check will be triggered every interval_in_seconds seconds to avoid flapping and
    overflowing sensu servers. If we fall behind and then recover, sensu will take care
    of resolving this alert itself.  Typically the producer will call process on this
    in this class for every message produced.

    Note the check_every variable in the sensu alert is defaulted to interval_in_seconds
    if it's not specified.

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
        self._service_name = service_name
        self._setup_ok_result_dict(result_dict)
        self._setup_delayed_result_dict()
        self._setup_disabled_alert_dict()
        self._log = logging.getLogger('{}.util.sensu_alert_manager'.format(service_name))
        self._disable = disable
        self._should_send_sensu_disabled_message = False
        self._max_delay = timedelta(seconds=max_delay_seconds)

    def _setup_ok_result_dict(self, result_dict):
        self._ok_result_dict = result_dict
        self._ok_result_dict['check_every'] = result_dict.get(
            'check_every',
            '{time}s'.format(time=self.interval_in_seconds)
        )
        self._ok_result_dict.update({
            'status': 0,
            'output': '{} has caught up to real time'.format(
                self._service_name
            )
        })

    def _setup_delayed_result_dict(self):
        self._delayed_result_dict = copy.deepcopy(self._ok_result_dict)
        self._delayed_result_dict.update({'status': 2})

    def _setup_disabled_alert_dict(self):
        self._disabled_alert_dict = copy.deepcopy(self._ok_result_dict)
        self._disabled_alert_dict.pop("ttl")
        self._disabled_alert_dict.update({
            'output': 'disabling sensu alert for {}'.format(self._service_name)
        })

    @property
    def disable(self):
        return self._disable

    @disable.setter
    def disable(self, new_value):
        # This condition means that something has toggled the disable variable from
        # False to True.  In this case we need to send sensu a message without a ttl
        # so it stops looking for ttl's.  If we didn't do this sensu would not get
        # any OK's so would keep alerting even if self._disable is True
        if not self._disable and new_value:
            self.log_and_send_event(self._disabled_alert_dict)
        self._disable = new_value

    def process(self, timestamp=None):
        if timestamp is None or self.disable:
            return

        # This timestamp param has to be timezone aware, otherwise it will not be
        # able to compare with timezone aware timestamps.
        delay_time = self._utc_now - timestamp
        if delay_time <= self._max_delay:
            self.log_and_send_event(self._ok_result_dict)
            return

        self._delayed_result_dict.update({
            'output': '{service} is falling {delay_time} min behind real time'.format(
                service=self._service_name,
                delay_time=delay_time),
        })
        self.log_and_send_event(self._delayed_result_dict)

    def log_and_send_event(self, output_dict):
        self._log.info("{} status: {}, output: {}.".format(
            self._service_name,
            output_dict['status'],
            output_dict['output'])
        )
        pysensu_yelp.send_event(**output_dict)
