# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.data_target import _DataTarget
from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of a consumer group.

Args:
    consumer_group_id (int): The id of the consumer group.
    group_name (str): The name of the consumer group.
    data_target (data_pipeline.schematizer_clientlib.models.data_target.DataTarget):
        The data_target this consumer group associates to.
    created_at (str): The timestamp when the consumer group is created in ISO-8601
        format.
    updated_at (str): The timestamp when the consumer group is last updated in ISO-8601
        format.
"""
ConsumerGroup = namedtuple(
    'ConsumerGroup',
    ['consumer_group_id', 'group_name', 'data_target', 'created_at', 'updated_at']
)


class _ConsumerGroup(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, consumer_group_id, group_name, data_target,
                 created_at, updated_at):
        self.consumer_group_id = consumer_group_id
        self.group_name = group_name
        self.data_target = data_target
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            consumer_group_id=response.consumer_group_id,
            group_name=response.group_name,
            data_target=_DataTarget.from_response(response.data_target),
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_cache_value(self):
        return {
            'consumer_group_id': self.consumer_group_id,
            'group_name': self.group_name,
            'data_target': self.data_target,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        id_only_data_target = _DataTarget(
            data_target_id=cache_value['data_target_id'],
            target_type=None,
            destination=None,
            created_at=None,
            updated_at=None
        )
        return cls(
            consumer_group_id=cache_value['consumer_group_id'],
            group_name=cache_value['group_name'],
            data_target=id_only_data_target,
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at']
        )

    def to_result(self):
        return ConsumerGroup(
            consumer_group_id=self.consumer_group_id,
            group_name=self.group_name,
            data_target=self.data_target.to_result(),
            created_at=self.created_at,
            updated_at=self.updated_at
        )
