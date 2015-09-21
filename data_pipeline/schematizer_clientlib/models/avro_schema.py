# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import simplejson

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.topic import _Topic


AvroSchema = namedtuple(
    'AvroSchema',
    ['schema_id', 'schema_json', 'topic', 'base_schema_id', 'created_at', 'updated_at']
)


class _AvroSchema(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, schema_id, schema_json, topic, base_schema_id,
                 created_at, updated_at):
        self.schema_id = schema_id
        self.schema_json = schema_json
        self.topic = topic
        self.base_schema_id = base_schema_id
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            schema_id=response.schema_id,
            schema_json=simplejson.loads(response.schema),
            topic=_Topic.from_response(response.topic),
            base_schema_id=response.base_schema_id,
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_cache_value(self):
        return {
            'schema_id': self.schema_id,
            'schema_json': self.schema_json,
            'topic_name': self.topic.name,
            'base_schema_id': self.base_schema_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        name_only_topic = _Topic(
            topic_id=None,
            name=cache_value['topic_name'],
            source=None,
            contains_pii=None,
            created_at=None,
            updated_at=None
        )
        return cls(
            schema_id=cache_value['schema_id'],
            schema_json=cache_value['schema_json'],
            topic=name_only_topic,
            base_schema_id=cache_value['base_schema_id'],
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at']
        )

    def to_result(self):
        return AvroSchema(
            schema_id=self.schema_id,
            schema_json=self.schema_json,
            topic=self.topic.to_result(),
            base_schema_id=self.base_schema_id,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
