# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of meta attribute mapping store. Meta attribute source
mapping store should represent a source and the corresponding meta attribute
schema id.
"""
MetaAttributeSourceMapping = namedtuple(
    'MetaAttributeSourceMapping',
    ['source_id', 'meta_attribute_schema_id']
)


class _MetaAttributeSourceMapping(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, source_id, meta_attribute_schema_id):
        self.source_id = source_id
        self.meta_attribute_schema_id = meta_attribute_schema_id

    @classmethod
    def from_response(cls, source_id, meta_attribute_schema_id):
        # Currently schematizer is not returning entity_id, entity_type. update
        # schematizer service to include that as well in MetaAttributeMapping
        # as well. taj.
        return cls(
            source_id=source_id,
            meta_attribute_schema_id=meta_attribute_schema_id
        )

    def to_result(self):
        return MetaAttributeSourceMapping(
            source_id=self.source_id,
            meta_attribute_schema_id=self.meta_attribute_schema_id
        )
