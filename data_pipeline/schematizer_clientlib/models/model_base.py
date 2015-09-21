# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy


class BaseModel(object):

    @classmethod
    def clone(cls):
        return copy.deepcopy(cls)
