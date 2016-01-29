# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from enum import Enum


class PublishGuaranteeEnum(Enum):
    """Enum that specifies what kind of message publishing guarantee provided
    by the producer.

    Attributes:
      exact_once: message will be successfully published exactly once.
      at_least_once: message will be successfully published at least once.
    """

    exact_once = 0
    at_least_once = 1
