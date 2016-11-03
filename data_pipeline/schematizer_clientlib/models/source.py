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
from data_pipeline.schematizer_clientlib.models.namespace import _Namespace


"""
Represent the data of a source.  Source is a sub-group under namespaces which
an avro schema is created for.  For example, `user` (table) could be a source.

Args:
    source_id (int): The id of the source.
    name (str): The name of the source.
    owner_email (str): The email of the source owner.
    namespace (data_pipeline.schematizer_clientlib.models.namespace.Namespace):
        The namespace of the source.
    category (str): The category of the source. (e.g. Content, Deals etc.)
"""
Source = namedtuple(
    'Source',
    ['source_id', 'name', 'owner_email', 'namespace', 'category']
)


class _Source(BaseModel):

    def __init__(self, source_id, name, owner_email, namespace, category):
        self.source_id = source_id
        self.name = name
        self.owner_email = owner_email
        self.namespace = namespace
        self.category = category

    @classmethod
    def from_response(cls, response):
        return cls(
            source_id=response.source_id,
            name=response.name,
            owner_email=response.owner_email,
            namespace=_Namespace.from_response(response.namespace),
            category=response.category
        )

    def to_cache_value(self):
        return {
            'source_id': self.source_id,
            'name': self.name,
            'owner_email': self.owner_email,
            'namespace': self.namespace,
            'category': self.category
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        return cls(
            source_id=cache_value['source_id'],
            name=cache_value['name'],
            owner_email=cache_value['owner_email'],
            namespace=cache_value['namespace'],
            category=cache_value['category']
        )

    def to_result(self):
        return Source(
            source_id=self.source_id,
            name=self.name,
            owner_email=self.owner_email,
            namespace=self.namespace.to_result(),
            category=self.category
        )
