# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
from collections import OrderedDict

class BaseIntrospectorModel(object):
    def __init__(self, model_obj, excluded_fields=None):
        if not excluded_fields:
            excluded_fields = []
        fields_to_grab = [
            field for field in model_obj._fields if field not in excluded_fields
        ]
        for field in fields_to_grab:
            value = getattr(model_obj, field)
            if isinstance(value, datetime.datetime):
                # datetime objects are not json serializable
                value = str(value)
            setattr(self, field, value)

    def to_ordered_dict(self):
        if not hasattr(self, '_fields'):
            raise NotImplementedError(
                "Derived class does not have a defined _fields "
                "attribute to define order of fields for dict"
            )
        result_dict = OrderedDict([])
        for field in self._fields:
            result_dict[field] = getattr(self, field)
        return result_dict
