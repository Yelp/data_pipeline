# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.source import _Source

"""
Represents the data of a refresh. A refresh represents the parameters
associated with an individual full refresh of a particular source.

Args:
    refresh_id (int): The id of the refresh.
    source (data_pipeline.schematizer_clientlib.models.source.Source):
        The source of the refresh.
    status (enum): The current status of the refresh.
        (One of: NOT_STARTED, IN_PROGRESS, PAUSED, SUCCESS, FAILED)
    offset (int): Last known offset that has been refreshed.
    batch_size (int): The number of rows to be refreshed per batch.
    priority (int): The priority of the refresh (1-100)
    filter_condition (str): The filter_condition associated with the refresh.
    created_at (str): The timestamp when the refresh is created in ISO-8601
        format.
    updated_at (str): The timestamp when the refresh is last updated in ISO-8601
        format.
"""
Refresh = namedtuple(
    'Refresh',
    [
        'refresh_id',
        'source',
        'status',
        'offset',
        'batch_size',
        'priority',
        'filter_condition',
        'created_at',
        'updated_at'
    ]
)


class _Refresh(BaseModel):

    def __init__(
        self,
        refresh_id,
        source,
        status,
        offset,
        batch_size,
        priority,
        filter_condition,
        created_at,
        updated_at
    ):
        self.refresh_id = refresh_id
        self.source = source
        self.status = status
        self.offset = offset
        self.batch_size = batch_size
        self.priority = priority
        self.filter_condition = filter_condition
        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_response(cls, response):
        return cls(
            refresh_id=response.refresh_id,
            source=_Source.from_response(response.source),
            status=response.status,
            offset=response.offset,
            batch_size=response.batch_size,
            priority=response.priority,
            filter_condition=response.filter_condition,
            created_at=response.created_at,
            updated_at=response.updated_at
        )

    def to_result(self):
        return Refresh(
            refresh_id=self.refresh_id,
            source=self.source.to_result(),
            status=self.status,
            offset=self.offset,
            batch_size=self.batch_size,
            priority=self.priority,
            filter_condition=self.filter_condition,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
