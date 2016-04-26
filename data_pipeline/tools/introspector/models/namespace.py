# -*- coding: utf-8 -*-
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
