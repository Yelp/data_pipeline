# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from enum import Enum


class DataSourceTypEnum(Enum):
    """Enum representing eligible data source types."""

    Namespace = 1
    Source = 2
