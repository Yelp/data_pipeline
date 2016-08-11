# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pysensu_yelp


class SensuTTLManager(object):
    """ This class triggers sensu alert if the producer falls behind real time.
    The check will be triggered every 30 seconds to avoid flapping and overflowing
    sensu servers. And if we fall behind and then recover, sensu will take care of
    resolving this alert itself.

    Args:
        result_dict(dict): dictionary with sensu parameters.  For details see
             http://pysensu-yelp.readthedocs.io/en/latest/index.html?highlight=send_event for details
        enable(bool): enable this ttl alert manager
    """

    def __init__(self, result_dict, enable=True):
        self._result_dict = result_dict
        self.enable = enable

    def process(self):
        if self.enable:
            pysensu_yelp.send_event(**self._result_dict)
