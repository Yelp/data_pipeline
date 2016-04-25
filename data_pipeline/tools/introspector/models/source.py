# -*- coding: utf-8 -*-
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

