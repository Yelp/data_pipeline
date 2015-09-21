# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.namespace import _Namespace


Source = namedtuple(
    'Source',
    ['source_id', 'name', 'owner_email', 'namespace']
)


class _Source(BaseModel):

    def __init__(self, source_id, name, owner_email, namespace):
        self.source_id = source_id
        self.name = name
        self.owner_email = owner_email
        self.namespace = namespace

    @classmethod
    def from_response(cls, response):
        return cls(
            source_id=response.source_id,
            name=response.source,
            owner_email=response.source_owner_email,
            namespace=_Namespace.from_response(response.namespace)
        )

    def to_cache_value(self):
        return {
            'source_id': self.source_id,
            'name': self.name,
            'owner_email': self.owner_email,
            'namespace': self.namespace
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        return cls(
            source_id=cache_value['source_id'],
            name=cache_value['name'],
            owner_email=cache_value['owner_email'],
            namespace=cache_value['namespace']
        )

    def to_result(self):
        return Source(
            source_id=self.source_id,
            name=self.name,
            owner_email=self.owner_email,
            namespace=self.namespace.to_result()
        )
