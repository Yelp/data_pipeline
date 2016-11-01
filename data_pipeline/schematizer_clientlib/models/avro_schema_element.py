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
from data_pipeline.schematizer_clientlib.models.note import _Note


"""
Represent the data of an Avro schema element.

Args:
    id (int): The element id.
    schema_id (int): The id of the avro schema.
    element_type (): The data type of the element
    element_name (str): The column corresponding to the key of the AvroSchemaElement
    doc ():
    note (Optional[data_pipeline.schematizer_clientlib.models.note.Note]): Information specified by users about the schema.
    created_at (str): The timestamp when the schema is created in ISO-8601
        format.
    updated_at (str): The timestamp when the schema is last updated in ISO-8601
        format.
"""

AvroSchemaElement = namedtuple(
    'AvroSchemaElement',
    ['id', 'schema_id', 'element_type', 'element_name', 'doc',
     'note', 'created_at', 'updated_at']
)

_SCHEMA_KEY_DELIMITER = '|'


class _AvroSchemaElement(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, id, schema_id, element_type, key, doc, note,
                 created_at, updated_at):
        self.id = id
        self.schema_id = schema_id
        self.element_type = element_type
        self.element_name = None
        self.doc = doc
        self.note = note
        self.created_at = created_at
        self.updated_at = updated_at
        split_keys = key.split(_SCHEMA_KEY_DELIMITER)
        if len(split_keys) >= 2:
            self.element_name = split_keys[1]

    @classmethod
    def from_response(cls, response_lst):
        res = []
        for response in response_lst:
            res.append(
                cls(
                    id=response.id,
                    schema_id=response.schema_id,
                    element_type=response.element_type,
                    key=response.key,
                    doc=response.doc,
                    note=_Note.from_response(response.note),
                    created_at=response.created_at,
                    updated_at=response.updated_at
                )
            )
        return res

    def to_result(self):
        return AvroSchemaElement(
            id=self.id,
            schema_id=self.schema_id,
            element_type=self.element_type,
            element_name=self.element_name,
            doc=self.doc,
            note=self.note.to_result() if self.note is not None else None,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
