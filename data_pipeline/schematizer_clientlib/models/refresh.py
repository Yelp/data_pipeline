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

from enum import Enum

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel
from data_pipeline.schematizer_clientlib.models.source import _Source

"""
Represents the data of a refresh. A refresh represents the parameters
associated with an individual full refresh of a particular source.

Args:
    refresh_id (int): The id of the refresh.
    source (data_pipeline.schematizer_clientlib.models.source.Source):
        The source of the refresh.
    status (RefreshStatus): The current status of the refresh.
    offset (int): Last known offset that has been refreshed.
    batch_size (int): The number of rows to be refreshed per batch.
    priority (Priority): The priority of the refresh
    filter_condition (str): The filter_condition associated with the refresh.
    avg_rows_per_second_cap (int): The throughput throttling cap to be used when
        the refresh is run.
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
        'avg_rows_per_second_cap',
        'created_at',
        'updated_at'
    ]
)


class Priority(Enum):
    LOW = 25
    MEDIUM = 50
    HIGH = 75
    MAX = 100


class RefreshStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    PAUSED = "PAUSED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


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
        updated_at,
        # Has to go last since it's optional
        avg_rows_per_second_cap=None
    ):
        self.refresh_id = refresh_id
        self.source = source
        self.status = RefreshStatus[status]
        self.offset = offset
        self.batch_size = batch_size
        self.priority = Priority[priority]
        self.filter_condition = filter_condition
        self.avg_rows_per_second_cap = avg_rows_per_second_cap
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
            avg_rows_per_second_cap=getattr(response, 'avg_rows_per_second_cap', None),
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
            avg_rows_per_second_cap=self.avg_rows_per_second_cap,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
