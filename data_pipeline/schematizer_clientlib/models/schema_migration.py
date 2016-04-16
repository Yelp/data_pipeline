# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from data_pipeline.schematizer_clientlib.models.model_base import BaseModel


"""
A SchemaMigration object contains a list of the SQL statements required to
migrate from an old schema to a new one.  In the event that there is no old
schema from which we need to migrate, the migration simply generates the
statements required to create a new table.

Args:
    migration_pushplan (list): A list containing SQL statements to be executed.
"""
SchemaMigration = namedtuple('SchemaMigration', ['migration_pushplan'])


class _SchemaMigration(BaseModel):

    def __init__(self, migration_pushplan):
        self.migration_pushplan = migration_pushplan

    @classmethod
    def from_response(cls, response):
        return cls(
            migration_pushplan=response
        )

    def to_result(self):
        return SchemaMigration(
            migration_pushplan=self.migration_pushplan
        )
