# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
Represent the data of an Avro schema element.

Args:
    id (int): The element id.
    schema_id (int): The id of the avro schema.
    element_type (): The data type of the element
    key ():
    doc ():
    note (Optional[str]): Information specified by users about the schema.
    created_at (str): The timestamp when the schema is created in ISO-8601
        format.
    updated_at (str): The timestamp when the schema is last updated in ISO-8601
        format.
"""

AvroSchemaElement = namedtuple(
    'AvroSchemaElement',
    ['id', 'schema_id', 'element_type', 'key', 'doc',
     'note', 'created_at', 'updated_at']
)


class _AvroSchemaElement(BaseModel):
    """Internal class used to convert from/to various data structure and
    facilitate constructing the return value of schematizer functions.
    """

    def __init__(self, id, schema_id, element_type, key, doc, note,
                 created_at, updated_at):
        self.id = id
        self.schema_id = schema_id
        self.element_type = element_type
        self.key = key
        self.doc = doc
        self.note = note
        self.created_at = created_at
        self.updated_at = updated_at

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
                    note=response.note,
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
            key=self.key,
            doc=self.doc,
            note=self.note,
            created_at=self.created_at,
            updated_at=self.updated_at
        )
