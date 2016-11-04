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

from data_pipeline_avro_util.avro_string_reader import AvroStringReader
from data_pipeline_avro_util.avro_string_writer import AvroStringWriter

from data_pipeline.helpers.singleton import Singleton
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class _AvroStringStore(object):
    """Singleton instance of store that caches
    AvroStringsWriter and AvroStringReader objects perticularly
    used by message class to encode and decode messages respectively.

    This class was added for performance enhancements
    w store : pb/199453
    w/o store : pb/199448
    """
    __metaclass__ = Singleton

    def __init__(self):
        self._writer_cache = {}
        self._reader_cache = {}

    @property
    def _schematizer(self):
        return get_schematizer()

    def _get_avro_schema(self, schema_id):
        return self._schematizer.get_schema_by_id(
            schema_id
        ).schema_json

    def get_writer(self, id_key, avro_schema=None):
        key = id_key
        avro_string_writer = self._writer_cache.get(key)
        if avro_string_writer:
            return avro_string_writer

        avro_schema = avro_schema or self._get_avro_schema(id_key)
        avro_string_writer = AvroStringWriter(schema=avro_schema)
        self._writer_cache[key] = avro_string_writer
        return avro_string_writer

    def get_reader(
        self,
        reader_id_key,
        writer_id_key,
        reader_avro_schema=None,
        writer_avro_schema=None
    ):
        key = reader_id_key, writer_id_key
        avro_string_reader = self._reader_cache.get(key)
        if avro_string_reader:
            return avro_string_reader

        reader_schema = (
            reader_avro_schema or self._get_avro_schema(reader_id_key)
        )
        writer_schema = (
            writer_avro_schema or self._get_avro_schema(writer_id_key)
        )
        avro_string_reader = AvroStringReader(
            reader_schema=reader_schema,
            writer_schema=writer_schema
        )
        self._reader_cache[key] = avro_string_reader
        return avro_string_reader
