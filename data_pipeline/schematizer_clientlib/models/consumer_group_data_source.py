# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of the mapping between a data source and a consumer group.
A data source represents a namespace or a source.

Args:
    consumer_group_data_source_id (int): The id of the mapping between consumer
        group and data source.
    consumer_group_id (str): The id of the consumer group.
    data_target_type
    (data_pipeline.schematizer_clientlib.models.data_source_type_enum.DataSourceTypeEnum):
        The type of the data_target.
    data_target_id: The id of the data target. It could be a namespace id or
        source id.
    created_at (str): The timestamp when the consumer group - data source mapping
        is created in ISO-8601 format.
    updated_at (str): The timestamp when the consumer group - data source mapping
        is last updated in ISO-8601 format.
"""
ConsumerGroupDataSource = namedtuple(
    'ConsumerGroupDataSource',
    ['consumer_group_data_source_id', 'consumer_group_id', 'data_target_type',
     'data_target_id', 'created_at', 'updated_at']
)


class _ConsumerGroupDataSource(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, consumer_group_data_source_id, consumer_group_id,
                 data_target_type, data_target_id, created_at, updated_at):
        self.consumer_group_data_source_id = consumer_group_data_source_id
        self.consumer_group_id = consumer_group_id
        self.data_target_type = data_target_type
        self.data_target_id = data_target_id
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            consumer_group_data_source_id=response.consumer_group_data_source_id,
            consumer_group_id=response.consumer_group_id,
            data_target_type=response.data_target_type,
            data_target_id=response.data_target_id,
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_result(self):
        return ConsumerGroupDataSource(
            consumer_group_data_source_id=self.consumer_group_data_source_id,
            consumer_group_id=self.consumer_group_id,
            data_target_type=self.data_target_type,
            data_target_id=self.data_target_id,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
