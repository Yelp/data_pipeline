# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pysensu_yelp


class SensuTTLManager(object):
    """ This class triggers sensu alert if the producer falls behind real time.
    The check will be triggered every 30 seconds to avoid flapping and overflowing
    sensu servers. And if we fall behind and then recover, sensu will take care of
    resolving this alert itself.

    inputs:
    result_dict --  dictionary with sensu parameters, for example:
        result_dict = {
            'name': 'replication_handler_real_time_check',
            'output': 'Replication Handler has caught up with real time.',
            'runbook': 'y/datapipeline',
            'status': 0,
            'team': 'bam',
            'page': False,
            'ttl': '300s',
            'sensu_host': 'paasta-cluster.yelp.',
            'source': 'replication_handler_environmentB_container_A'
        }
    disable -- boolean to disable sensu
    """

    def __init__(self, result_dict, disable=False):
        self._result_dict = result_dict
        self.enable = not disable

    def process(self):
        if self.enable:
            pysensu_yelp.send_event(**self._result_dict)
