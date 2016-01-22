# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.data_target import _DataTarget
from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of a consumer group.  A consumer group represents a group of
the consumers the messages which consume will deliver to the same destination.
Such destination is defined as a "data target".

Args:
    consumer_group_id (int): The id of the consumer group.
    group_name (str): The name of the consumer group.
    data_target (data_pipeline.schematizer_clientlib.models.data_target.DataTarget):
        The data_target this consumer group associates to.
"""
ConsumerGroup = namedtuple(
    'ConsumerGroup',
    ['consumer_group_id', 'group_name', 'data_target']
)


class _ConsumerGroup(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, consumer_group_id, group_name, data_target):
        self.consumer_group_id = consumer_group_id
        self.group_name = group_name
        self.data_target = data_target

    @classmethod
    def from_response(cls, response):
        return cls(
            consumer_group_id=response.consumer_group_id,
            group_name=response.group_name,
            data_target=_DataTarget.from_response(response.data_target)
        )

    def to_cache_value(self):
        return {
            'consumer_group_id': self.consumer_group_id,
            'group_name': self.group_name,
            'data_target_id': self.data_target.data_target_id
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        id_only_data_target = _DataTarget(
            data_target_id=cache_value['data_target_id'],
            target_type=None,
            destination=None
        )
        return cls(
            consumer_group_id=cache_value['consumer_group_id'],
            group_name=cache_value['group_name'],
            data_target=id_only_data_target
        )

    def to_result(self):
        return ConsumerGroup(
            consumer_group_id=self.consumer_group_id,
            group_name=self.group_name,
            data_target=self.data_target.to_result()
        )
