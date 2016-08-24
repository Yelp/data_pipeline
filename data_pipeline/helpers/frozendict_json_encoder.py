# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json

from frozendict import frozendict


class FrozenDictEncoder(json.JSONEncoder):
    """Custom json encoder for encoding frozendict objects
    """
    def default(self, obj):
        if isinstance(obj, frozendict):
            return dict(obj)
        return json.JSONEncoder.default(self, obj)
