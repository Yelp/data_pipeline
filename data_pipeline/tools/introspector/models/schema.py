# -*- coding: utf-8 -*-
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
