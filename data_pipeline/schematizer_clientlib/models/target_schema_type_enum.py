# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from enum import Enum


class TargetSchemaTypeEnum(Enum):
    """Eligible target schema types."""

    unsupported = 0
    redshift = 1
