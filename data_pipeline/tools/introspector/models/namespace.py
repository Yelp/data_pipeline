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

from data_pipeline.tools.introspector.models.base import BaseIntrospectorModel


class IntrospectorNamespace(BaseIntrospectorModel):
    def __init__(self, namespace_obj, active_namespaces=None):
        super(IntrospectorNamespace, self).__init__(
            namespace_obj
        )
        self._fields = [
            'name', 'namespace_id'
        ]
        # Need to check for none in case of empty list
        if active_namespaces is not None:
            self._fields.append('active_source_count')
            self._fields.append('active_topic_count')
            self.active_source_count = 0
            self.active_topic_count = 0
            active_namespace = active_namespaces.get(self.name, None)
            if active_namespace:
                self.active_source_count = active_namespace['active_source_count']
                self.active_topic_count = active_namespace['active_topic_count']
