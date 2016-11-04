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
Represent the data of a data target.  A data target represents a destination
where the data(messages) are eventually sent to, such as a Redshift cluster.

Args:
    data_target_id (int): The id of the data target.
    target_type (str): The target type, such as Redshift, etc.
    destination (str): The actual location of the data target, such as the Url
        of a Redshift cluster.
"""
DataTarget = namedtuple(
    'DataTarget',
    ['data_target_id', 'name', 'target_type', 'destination']
)


class _DataTarget(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, data_target_id, name, target_type, destination):
        self.data_target_id = data_target_id
        self.name = name
        self.target_type = target_type
        self.destination = destination

    @classmethod
    def from_response(cls, response):
        return cls(
            data_target_id=response.data_target_id,
            name=response.name,
            target_type=response.target_type,
            destination=response.destination
        )

    def to_cache_value(self):
        return {
            'data_target_id': self.data_target_id,
            'name': self.name,
            'target_type': self.target_type,
            'destination': self.destination
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        return cls(
            data_target_id=cache_value['data_target_id'],
            name=cache_value['name'],
            target_type=cache_value['target_type'],
            destination=cache_value['destination']
        )

    def to_result(self):
        return DataTarget(
            data_target_id=self.data_target_id,
            name=self.name,
            target_type=self.target_type,
            destination=self.destination
        )
