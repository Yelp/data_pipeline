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
from data_pipeline.tools.introspector.models.topic import IntrospectorTopic


class IntrospectorSchema(BaseIntrospectorModel):
    def __init__(self, schema_obj, include_topic_info=False):
        super(IntrospectorSchema, self).__init__(
            schema_obj
        )
        self._fields = [
            'schema_id', 'base_schema_id', 'status',
            'primary_keys', 'created_at', 'note', 'schema_json'
        ]
        if include_topic_info:
            self._fields.append('topic')
            self.topic = IntrospectorTopic(self.topic).to_ordered_dict()
