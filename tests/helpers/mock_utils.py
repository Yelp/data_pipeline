# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock


def attach_spy_on_func(target, attribute):
    orig_func = getattr(target, attribute)

    def attach_spy(*args, **kwargs):
        return orig_func(*args, **kwargs)

    return mock.patch.object(target, attribute, side_effect=attach_spy)
