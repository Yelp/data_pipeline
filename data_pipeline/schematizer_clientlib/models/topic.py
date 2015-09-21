# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.source import _Source


Topic = namedtuple(
    'Topic',
    ['topic_id', 'name', 'source', 'contains_pii', 'created_at', 'updated_at']
)


class _Topic(BaseModel):

    def __init__(self, topic_id, name, source, contains_pii,
                 created_at, updated_at):
        self.topic_id = topic_id
        self.name = name
        self.source = source
        self.contains_pii = contains_pii
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            topic_id=response.topic_id,
            name=response.name,
            source=_Source.from_response(response.source),
            contains_pii=response.contains_pii,
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_cache_value(self):
        return {
            'topic_id': self.topic_id,
            'name': self.name,
            'source_id': self.source.source_id,
            'contains_pii': self.contains_pii,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        id_only_source = _Source(
            source_id=cache_value['source_id'],
            name=None,
            owner_email=None,
            namespace=None
        )
        return cls(
            topic_id=cache_value['topic_id'],
            name=cache_value['name'],
            source=id_only_source,
            contains_pii=cache_value['contains_pii'],
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at']
        )

    def to_result(self):
        return Topic(
            topic_id=self.topic_id,
            name=self.name,
            source=self.source.to_result(),
            contains_pii=self.contains_pii,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
