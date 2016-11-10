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

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.source import _Source


"""
Represent the data of a topic.  A topic represents a kafka topic which a
producer publishes messages to (or a consumer consumes messages from).

Args:
    topic_id (int): The id of the topic.
    name (str): The name of the topic.
    source (data_pipeline.schematizer_clientlib.models.source.Source):
        The source of the topic.
    contains_pii (bool): Whether the messages in this topic contain PII data.
    cluster_type (str): Kafka cluster type to connect. Ex datapipe, scribe, etc.
    primary_keys ([str]): List of keys that are the primary keys of the topics schemas
    created_at (str): The timestamp when the topic is created in ISO-8601
        format.
    updated_at (str): The timestamp when the topic is last updated in ISO-8601
        format.
"""
Topic = namedtuple(
    'Topic',
    ['topic_id', 'name', 'source', 'contains_pii', 'cluster_type',
     'primary_keys', 'created_at', 'updated_at']
)


class _Topic(BaseModel):

    def __init__(
        self,
        topic_id,
        name,
        source,
        contains_pii,
        cluster_type,
        primary_keys,
        created_at,
        updated_at
    ):
        self.topic_id = topic_id
        self.name = name
        self.source = source
        self.contains_pii = contains_pii
        self.cluster_type = cluster_type
        self.primary_keys = primary_keys
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            topic_id=response.topic_id,
            name=response.name,
            source=_Source.from_response(response.source),
            contains_pii=response.contains_pii,
            cluster_type=response.cluster_type,
            primary_keys=response.primary_keys,
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_cache_value(self):
        return {
            'topic_id': self.topic_id,
            'name': self.name,
            'source_id': self.source.source_id,
            'contains_pii': self.contains_pii,
            'cluster_type': self.cluster_type,
            'primary_keys': self.primary_keys,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        id_only_source = _Source(
            source_id=cache_value['source_id'],
            name=None,
            owner_email=None,
            namespace=None,
            category=None
        )
        return cls(
            topic_id=cache_value['topic_id'],
            name=cache_value['name'],
            source=id_only_source,
            contains_pii=cache_value['contains_pii'],
            cluster_type=cache_value['cluster_type'],
            primary_keys=cache_value['primary_keys'],
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at']
        )

    def to_result(self):
        return Topic(
            topic_id=self.topic_id,
            name=self.name,
            source=self.source.to_result(),
            contains_pii=self.contains_pii,
            cluster_type=self.cluster_type,
            primary_keys=self.primary_keys,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
