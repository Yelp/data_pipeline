# -*- coding: utf-8 -*-
""" Placeholder utility module for testing with provided sample data -
    The library will not actually do any encoding/decoding itself this will be
    provided by the data pipeline client lib
"""
# TODO(joshszep#DATAPIPE-131|2015-04-21): Integrate data pipeline clientlib
from __future__ import absolute_import
from __future__ import unicode_literals

import io

import avro.io
import avro.schema
import simplejson


def decode_payload(payload, schema):    # pragma: no cover
    bytes_reader = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(_get_avro_schema_object(schema))
    return reader.read(decoder)


def encode_payload(data, schema):    # pragma: no cover
    writer = avro.io.DatumWriter(
        writers_schema=_get_avro_schema_object(schema))
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()


def _get_avro_schema_object(schema):    # pragma: no cover
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
