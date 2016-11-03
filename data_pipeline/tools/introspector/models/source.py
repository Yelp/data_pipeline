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


class IntrospectorSource(BaseIntrospectorModel):
    def __init__(self, source_obj, active_sources=None):
        super(IntrospectorSource, self).__init__(
            source_obj
        )
        self._fields = [
            'name', 'source_id', 'owner_email', 'namespace'
        ]
        self.namespace = source_obj.namespace.name
        # Need to check for none in case of empty list
        if active_sources is not None:
            self._fields.append('active_topic_count')
            active_source = active_sources.get(source_obj.source_id, None)
            self.active_topic_count = 0 if (
                not active_source
            ) else active_source['active_topic_count']
