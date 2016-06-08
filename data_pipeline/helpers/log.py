# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from data_pipeline.config import get_config


def debug_log(line_lambda, exc_info=None):
    """This avoids unnecessary formatting of debug log string.
    More info in DATAPIPE-979
    """
    if get_config().logger.isEnabledFor(logging.DEBUG):
        get_config().logger.debug(line_lambda(), exc_info=exc_info)
