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
	used my message class to encode and decode messages respectively.

	This aids significant performance improvements in data pipeline.
	"""
    __metaclass__ = Singleton

    def __init__(self):
        self._cache_writer = {}
        self._cache_reader = {}

    def _make_avro_schema_key(self, avro_schema):
        return simplejson.dumps(avro_schema, sort_keys=True)

    def get_writer(self, avro_schema):
        key = self._make_avro_schema_key(avro_schema)
        if key in self._cache_writer:
            return self._cache_writer[key]
        else:
            avro_string_writer = AvroStringWriter(
                schema=avro_schema
            )
            self._cache_writer[key] = avro_string_writer
            return avro_string_writer

    def get_reader(self, reader_schema, writer_schema):
        key1 = self._make_avro_schema_key(reader_schema)
        key2 = self._make_avro_schema_key(writer_schema)
        key = "{0}_{1}".format(key1, key2)
        if key in self._cache_reader:
            return self._cache_reader[key]
        else:
            avro_string_reader = AvroStringReader(
                reader_schema=reader_schema,
                writer_schema=writer_schema
            )
            self._cache_reader[key] = avro_string_reader
            return avro_string_reader
