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

from enum import Enum


class ExpectedFrequency(Enum):
    """Helper constants specifying how frequently the client expects to produce
    or consume messages.  Any positive integer number of seconds can be used,
    these are provided for convenience only.  Expected frequency will be used
    to infer schema deprecation.

    For example, if a client registers to produce messages constantly, and a
    few months later we observe that the client hasn't published messages using
    an older schema verison in a month, but does regularly publish using a
    newer version, we can infer that the older schema version is deprecated
    and send out a deprecation/migration notice.

    Attributes:
      constantly: client expects to always and continuously be producing and
        consuming messages.
      hourly: client expects to come online to produce or consume messages
        approximately every hour.
      weekly: client expects to come online to produce or consume messages about
        once a week.
      monthly: client expects to come online to produce or consume messages about
        once a month.
      yearly: client expects to come online to produce or consume messages about
        once a year.
    """
    constantly = 0
    hourly = 60 * 60
    daily = hourly * 24
    weekly = daily * 7
    monthly = daily * 30
    yearly = daily * 365
