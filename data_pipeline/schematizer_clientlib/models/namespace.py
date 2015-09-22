# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of a namespace.  Namespace is a group which the avro schemas
belong to.  It is the highest grouping level of schemas.  For example,
`yelp_main` could be a namespace.

Args:
    namespace_id (int): The id of the namespace.
    name (str): The name of the namespace.
"""
Namespace = namedtuple('Namespace', ['namespace_id', 'name'])


class _Namespace(BaseModel):

    def __init__(self, namespace_id, name):
        self.namespace_id = namespace_id
        self.name = name

    @classmethod
    def from_response(cls, response):
        return cls(
            namespace_id=response.namespace_id,
            name=response.name
        )

    def to_result(self):
        return Namespace(
            namespace_id=self.namespace_id,
            name=self.name
        )
