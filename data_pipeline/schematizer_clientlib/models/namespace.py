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
