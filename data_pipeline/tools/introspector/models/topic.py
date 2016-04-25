# -*- coding: utf-8 -*-
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
            'primary_keys', 'contains_pii'
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
