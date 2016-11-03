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

import pysensu_yelp


class SensuTTLAlerter(object):
    """ This class triggers a sensu alert if the producer dies.  If the sensu_event_info is
    not sent within the ttl specified the prior time it was sent then sensu will alert.

    Args:
        sensu_event_info(dict): dictionary with sensu parameters.  For details see
             http://pysensu-yelp.readthedocs.io/en/latest/index.html?highlight=send_event
             for details
        enable(bool): enable this ttl alert manager
    """

    def __init__(self, sensu_event_info, enable=True):
        self._sensu_event_info = sensu_event_info
        self._enable = enable

    def process(self):
        if self.enable:
            pysensu_yelp.send_event(**self._sensu_event_info)

    @property
    def enable(self):
        return self._enable

    @enable.setter
    def enable(self, new_enable_value):
        if self._enable and not new_enable_value:
            # send final message without ttl
            final_sensu_info = copy.deepcopy(self._sensu_event_info)
            final_sensu_info.pop('ttl')
            pysensu_yelp.send_event(**final_sensu_info)
        self._enable = new_enable_value
