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


class IntrospectorTopic(BaseIntrospectorModel):
    def __init__(self, topic_obj, kafka_topics=None, topics_to_range_map=None):
        super(IntrospectorTopic, self).__init__(
            topic_obj,
            excluded_fields=['source']
        )
        self._fields = [
            'name', 'topic_id', 'source_name', 'source_id', 'namespace',
            'primary_keys', 'contains_pii', 'cluster_type'
        ]
        self.source_name = topic_obj.source.name
        self.source_id = topic_obj.source.source_id
        self.namespace = topic_obj.source.namespace.name
        if kafka_topics is not None:
            self.in_kafka = self.name in kafka_topics
            self._fields.append('in_kafka')
        if topics_to_range_map is not None:
            self.message_count = self._get_topic_message_count(
                topics_to_range_map
            )
            if self.message_count:
                self.in_kafka = True
            self._fields.append('message_count')
        self._fields.extend(['created_at', 'updated_at'])

    def _get_topic_message_count(self, topics_to_range_map):
        if self.name in topics_to_range_map:
            return sum(topics_to_range_map[self.name].values())
        return 0
