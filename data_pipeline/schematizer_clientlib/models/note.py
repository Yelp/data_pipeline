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
Represent the data of a note. A note is used to document the data.
For example, the text used to explain the meaning of the is_inactive column.

Args:
    id (int): the id of the note.
    reference id (int): the id of the entity that this note describes
    reference_type (str): indicate the type of the entity (schema or schema_elements)
    updated_at (timestamp): the updated time
    note (str): the text of this note
    last_updated_by (str): the entity that last updated this note
    created_at (timestamp): the time that this note was created
"""

Note = namedtuple(
    'Note',
    [
        'created_at',
        'reference_type',
        'updated_at',
        'note',
        'last_updated_by',
        'reference_id',
        'id'
    ]
)


class _Note(BaseModel):
    """Internal class used to convert from/to various data structures and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(
        self,
        id,
        reference_id,
        reference_type,
        created_at,
        updated_at,
        note,
        last_updated_by
    ):
        self.id = id
        self.reference_id = reference_id
        self.reference_type = reference_type
        self.created_at = created_at
        self.updated_at = updated_at
        self.note = note
        self.last_updated_by = last_updated_by

    @classmethod
    def from_response(cls, response):
        if response is None:
            return None
        return cls(
            id=response.id,
            reference_id=response.reference_id,
            reference_type=response.reference_type,
            created_at=response.created_at,
            updated_at=response.updated_at,
            note=response.note,
            last_updated_by=response.last_updated_by
        )

    def to_cache_value(self):
        return {
            'id': self.id,
            'reference_id': self.reference_id,
            'reference_type': self.reference_type,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'note': self.note,
            'last_updated_by': self.last_updated_by
        }

    @classmethod
    def from_cache_value(cls, cache_value):
        if cache_value is None:
            return None
        return cls(
            id=cache_value['id'],
            reference_id=cache_value['reference_id'],
            reference_type=cache_value['reference_type'],
            created_at=cache_value['created_at'],
            updated_at=cache_value['updated_at'],
            note=cache_value['note'],
            last_updated_by=cache_value['last_updated_by']
        )

    def to_result(self):
        return Note(
            id=self.id,
            reference_id=self.reference_id,
            reference_type=self.reference_type,
            created_at=self.created_at,
            updated_at=self.updated_at,
            note=self.note,
            last_updated_by=self.last_updated_by
        )
