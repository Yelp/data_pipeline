# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import simplejson
from frozendict import frozendict

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.note import _Note
from data_pipeline.schematizer_clientlib.models.topic import _Topic


"""
Represent the data of an Avro schema.

Args:
    schema_id (int): The id of the avro schema.
    schema_json (dict or list): The Python representation of the avro schema.
    topic (data_pipeline.schematizer_clientlib.models.topic.Topic): The topic
        of the avro schema.
    base_schema_id (Optional[int]): The id of the base schema which this avro
        schema is changed based on.  `None` if there is no such base schema.
    status (string): The status of the schema.  It could be: "RW" (read/write),
        "R" (read-only), or "Disabled".  Read status means the schema can be
        used to deserialize messages, and Write status means the schema can be
        used to serialize messages.
    primary_keys (list): List of primary key names.
    note (Optional[data_pipeline.schematizer_clientlib.models.note.Note]): Information specified by users about the schema.
    created_at (str): The timestamp when the schema is created in ISO-8601
        format.
    updated_at (str): The timestamp when the schema is last updated in ISO-8601
        format.
"""
AvroSchema = namedtuple(
    'AvroSchema',
    ['schema_id', 'schema_json', 'topic', 'base_schema_id', 'status',
     'primary_keys', 'note', 'created_at', 'updated_at']
)


class _AvroSchema(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, schema_id, schema_json, topic, base_schema_id, status,
                 primary_keys, note, created_at, updated_at):
        self.schema_id = schema_id
        self.schema_json = schema_json
        self.topic = topic
        self.base_schema_id = base_schema_id
        self.status = status
        self.primary_keys = primary_keys
        self.note = note
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            schema_id=response.schema_id,
            schema_json=simplejson.loads(response.schema),
            topic=_Topic.from_response(response.topic),
            base_schema_id=response.base_schema_id,
            status=response.status,
            primary_keys=response.primary_keys,
            note=_Note.from_response(response.note),
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_cache_value(self):
        return {
            'schema_id': self.schema_id,
            'schema_json': frozendict(self.schema_json),
            'topic_name': self.topic.name,
            'base_schema_id': self.base_schema_id,
            'status': self.status,
            'primary_keys': self.primary_keys,
            'note': frozendict(self.note.to_cache_value()) if self.note is not None else None,
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
            cluster_type=None,
            primary_keys=None,
            created_at=None,
            updated_at=None
        )
        return cls(
            schema_id=cache_value['schema_id'],
            schema_json=cache_value['schema_json'],
            topic=name_only_topic,
            base_schema_id=cache_value['base_schema_id'],
            status=cache_value['status'],
            primary_keys=cache_value['primary_keys'],
            note=_Note.from_cache_value(cache_value['note']),
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at']
        )

    def to_result(self):
        return AvroSchema(
            schema_id=self.schema_id,
            schema_json=self.schema_json,
            topic=self.topic.to_result(),
            base_schema_id=self.base_schema_id,
            status=self.status,
            primary_keys=self.primary_keys,
            note=self.note.to_result() if self.note is not None else None,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
