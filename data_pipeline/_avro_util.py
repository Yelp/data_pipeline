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
        """ Utility class for encoding Avro.
        Args:
            schema (string|dict|:class:`avro.schema.Schema`): An avro schema
                for encoding.

        Notes:
            The `schema` arg may be given in any of these forms:
                - An avro json string
                - An avro dict representation (parsed json string)
                - An :class:`avro.schema.Schema` object
        """
        self.schema = get_avro_schema_object(schema)

    @cached_property
    def avro_writer(self):
        return avro.io.DatumWriter(
            writers_schema=self.schema
        )

    def encode(self, message_avro_representation):
        """ Encodes a given `message_avro_representation` using `self.schema`.

        Args:
            message_avro_representation (dict): A dictionary which matches the
                schema defined by `self.schema`

        Returns (string):
            An encoded bytes representation.
        """
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
        """ Utility class for decoding Avro.

        Args:
            reader_schema (string|dict|:class:`avro.schema.Schema`): An avro
                schema for decoding, which represents the object you wish to
                decode into. Must be backwards compatible with `writer_schema`.
            writer_schema (string|dict|:class:`avro.schema.Schema`): An avro
                schema for decoding, which represents the object the data was
                originally encoded with.

        Notes:
            Both the `reader_schema` and `writer_schema` args may be given in
            any of these forms:
                - An avro json string
                - An avro dict representation (parsed json string)
                - An :class:`avro.schema.Schema` object
        """
        self.reader_schema = get_avro_schema_object(reader_schema)
        self.writer_schema = get_avro_schema_object(writer_schema)

    @cached_property
    def avro_reader(self):
        return avro.io.DatumReader(
            readers_schema=self.reader_schema,
            writers_schema=self.writer_schema
        )

    def decode(self, encoded_message):
        """ Decodes a given `encoded_message` which was encoded using the
        same schema as `self.writer_schema` into a representation defined by
        `self.reader_schema`.

        Args:
            encoded_message (string): An encoded object

        Returns (dict):
            The decoded dictionary representation.
        """
        stringio = cStringIO.StringIO(encoded_message)
        decoder = avro.io.BinaryDecoder(stringio)
        return self.avro_reader.read(decoder)


def get_avro_schema_object(schema):
    """ Helper function to simplify dealing with the three ways avro schema may
        be represented:
        - a json string
        - a dictionary (parsed json string)
        - a parsed `avro.schema.Schema` object

        In all cases this returns the `avro.schema.Schema` object form
    """
    if isinstance(schema, avro.schema.Schema):
        return schema
    elif isinstance(schema, basestring):
        return avro.schema.parse(schema)
    else:
        return avro.schema.parse(simplejson.dumps(schema))


_avro_primitive_type_to_example_value = {
    'null': None,
    'boolean': True,
    'string': '❤',
    'bytes': b'_',
    'int': 1,
    'long': 2,
    'float': 0.5,  # 0.5 works for a == b comparisons after avro encode/decode
    'double': 2.2,
}


def generate_payload_data(schema, data_spec={}):
    """ Generate a valid payload data dict for a given avro schema, with an
    optional data spec to override defaults.

    Args:
        schema (avro.schema.RecordSchema): An avro schema
        data_spec (dict): {field_name: value} dictionary of values to use
            in the resulting payload data dict

    Returns (dict):
        A valid payload data example
    """
    assert isinstance(schema, avro.schema.RecordSchema)
    data = {}
    for field in schema.fields:
        data[field.name] = data_spec.get(
            field.name,
            generate_field_value(field)
        )
    return data


def generate_field_value(field):
    """ Generate a value for a given avro schema field. If the field has a
    default value specified, that is used, otherwise the first PrimitiveSchema
    definition is used to generate a default valid value.

    Args:
        field (avro.schema.Field): An avro field

    Returns:
        A value which is valid for the given field.
    """
    assert isinstance(field, avro.schema.Field)

    primitive_type = get_field_primitive_type(field)

    if field.has_default:
        if primitive_type == 'bytes':
            return bytes(field.default)
        else:
            return field.default
    else:
        return _avro_primitive_type_to_example_value[primitive_type]


def get_field_primitive_type(field):
    """ The first PrimitiveSchema definition is used to return the primitive
    type of the field, dealing with single-layer unions

    Args:
        field (avro.schema.Field): An avro field

    Returns (str): the primitve field type
    """
    assert isinstance(field, avro.schema.Field)
    if isinstance(field.type, avro.schema.UnionSchema):
        for schema in field.type.schemas:
            if isinstance(schema, avro.schema.PrimitiveSchema):
                return schema.type
    elif isinstance(field.type, avro.schema.PrimitiveSchema):
        return field.type.type
