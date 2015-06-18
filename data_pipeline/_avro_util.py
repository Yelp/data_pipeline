# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import cStringIO

import avro.io
import avro.schema
import simplejson
from cached_property import cached_property


class AvroStringWriter(object):
    def __init__(self, schema):
        self.schema = schema

    @cached_property
    def avro_writer(self):
        return avro.io.DatumWriter(self.schema)

    def encode(self, message_avro_representation):
        # Benchmarking this revealed that recreating stringio and the encoder
        # isn't slower than truncating the stringio object.  This is supported
        # by benchmarks that indicate it's faster to instantiate a new object
        # than truncate an existing one:
        # http://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
        stringio = cStringIO.StringIO()
        encoder = avro.io.BinaryEncoder(stringio)
        self.avro_writer.write(message_avro_representation, encoder)
        return stringio.getvalue()


class AvroStringReader(object):
    def __init__(self, reader_schema, writer_schema):
        self.reader_schema = reader_schema
        self.writer_schema = writer_schema

    @cached_property
    def avro_reader(self):
        return avro.io.DatumReader(
            readers_schema=self.reader_schema,
            writers_schema=self.writer_schema
        )

    def decode(self, encoded_message):
        stringio = cStringIO.StringIO(encoded_message)
        decoder = avro.io.BinaryDecoder(stringio)
        return self.avro_reader.read(decoder)


def get_avro_schema_object(schema):
    """ Helper function to simplify dealing with the three ways avro schema may
        be represented:
        - a json string
        - a dictionary (parsed json string)
        - a parsed avro schema object"""
    if isinstance(schema, avro.schema.Schema):
        return schema
    elif isinstance(schema, basestring):
        return avro.schema.parse(schema)
    else:
        return avro.schema.parse(simplejson.dumps(schema))
