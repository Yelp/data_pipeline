# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson

from yelp_avro.avro_string_reader import AvroStringReader
from yelp_avro.avro_string_writer import AvroStringWriter

from data_pipeline.helpers.singleton import Singleton


class AvroStringStore(object):
    """Singleton instance of store that caches 
    AvroStringsWriter and AvroStringReader objects perticularly 
    used by message class to encode and decode messages respectively.

    This class was added for performance enhancements
    before : https://pb.yelpcorp.com/199097
    after : https://pb.yelpcorp.com/199143
    """
    __metaclass__ = Singleton

    def __init__(self):
        self._writer_cache = {}
        self._reader_cache = {}

    def get_writer(self, schema_id, avro_schema):
        key = schema_id
        if key in self._writer_cache:
            return self._writer_cache[key]
        else:
            avro_string_writer = AvroStringWriter(
                schema=avro_schema
            )
            self._writer_cache[key] = avro_string_writer
            return avro_string_writer

    def get_reader(self, schema_id, reader_schema, writer_schema):
        key = schema_id
        if key in self._reader_cache:
            return self._reader_cache[key]
        else:
            avro_string_reader = AvroStringReader(
                reader_schema=reader_schema,
                writer_schema=writer_schema
            )
            self._reader_cache[key] = avro_string_reader
            return avro_string_reader
