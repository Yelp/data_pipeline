# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of a data target.

Args:
    data_target_id (int): The id of the data target.
    target_type (str): The target type, such as Redshift, etc.
    destination (str): The actual location of the data target, such as the Url
        of the Redshift cluster.
    created_at (str): The timestamp when the data target is created in ISO-8601
        format.
    updated_at (str): The timestamp when the data target is last updated in ISO-8601
        format.
"""
DataTarget = namedtuple(
    'DataTarget',
    ['data_target_id', 'target_type', 'destination', 'created_at', 'updated_at']
)


class _DataTarget(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, data_target_id, target_type, destination,
                 created_at, updated_at):
        self.data_target_id = data_target_id
        self.target_type = target_type
        self.destination = destination
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            data_target_id=response.data_target_id,
            target_type=response.target_type,
            destination=response.destination,
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_cache_value(self):
        return {
            'data_target_id': self.data_target_id,
            'target_type': self.target_type,
            'destination': self.destination,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        return cls(
            data_target_id=cache_value['data_target_id'],
            target_type=cache_value['target_type'],
            destination=cache_value['destination'],
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at']
        )

    def to_result(self):
        return DataTarget(
            data_target_id=self.data_target_id,
            target_type=self.target_type,
            destination=self.destination,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
