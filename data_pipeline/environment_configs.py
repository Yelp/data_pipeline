# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

IS_OPEN_SOURCE_MODE = os.getenv('OPEN_SOURCE_MODE', 'false').lower() in ['t', 'true', 'y', 'yes']
