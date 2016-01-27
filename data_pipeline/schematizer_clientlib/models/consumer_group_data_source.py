# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.data_source_type_enum import DataSourceTypeEnum
from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of the mapping between a data source and a consumer group.
A data source represents a namespace or a source.

Args:
    consumer_group_data_source_id (int): The id of the mapping between consumer
        group and data source.
    consumer_group_id (str): The id of the consumer group.
    data_source_type
    (data_pipeline.schematizer_clientlib.models.data_source_type_enum.DataSourceTypeEnum):
        The type of the data_source.
    data_source_id: The id of the data target.  Depending on the data source
        type, it may be a namespace id or source id.
"""
ConsumerGroupDataSource = namedtuple(
    'ConsumerGroupDataSource',
    ['consumer_group_data_source_id', 'consumer_group_id', 'data_source_type',
     'data_source_id']
)


class _ConsumerGroupDataSource(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, consumer_group_data_source_id, consumer_group_id,
                 data_source_type, data_source_id):
        self.consumer_group_data_source_id = consumer_group_data_source_id
        self.consumer_group_id = consumer_group_id
        self.data_source_type = data_source_type
        self.data_source_id = data_source_id

    @classmethod
    def from_response(cls, response):
        return cls(
            consumer_group_data_source_id=response.consumer_group_data_source_id,
            consumer_group_id=response.consumer_group_id,
            data_source_type=DataSourceTypeEnum[response.data_source_type],
            data_source_id=response.data_source_id
        )

    def to_result(self):
        return ConsumerGroupDataSource(
            consumer_group_data_source_id=self.consumer_group_data_source_id,
            consumer_group_id=self.consumer_group_id,
            data_source_type=self.data_source_type,
            data_source_id=self.data_source_id
        )
