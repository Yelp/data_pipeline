# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of meta attribute mapping store. Meta attribute namespace
mapping should represent a mapping of a namespace and the corresponding meta
attribute schema id.
"""
MetaAttributeNamespaceMapping = namedtuple(
    'MetaAttributeNamespaceMapping',
    ['namespace_id', 'meta_attribute_schema_id']
)


class _MetaAttributeNamespaceMapping(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, namespace_id, meta_attribute_schema_id):
        self.namespace_id = namespace_id
        self.meta_attribute_schema_id = meta_attribute_schema_id

    @classmethod
    def from_response(cls, namespace_id, meta_attribute_schema_id):
        return cls(
            namespace_id=namespace_id,
            meta_attribute_schema_id=meta_attribute_schema_id
        )

    def to_result(self):
        return MetaAttributeNamespaceMapping(
            namespace_id=self.namespace_id,
            meta_attribute_schema_id=self.meta_attribute_schema_id
        )
