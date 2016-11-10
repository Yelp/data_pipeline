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
