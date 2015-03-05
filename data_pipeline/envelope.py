from cached_property import cached_property

import avro.io
import avro.schema
import cStringIO
import os


class Envelope(object):
    @cached_property
    def schema(self):
        schema_path = os.path.join(
            os.path.dirname(__file__),
            '../schemas/envelope_v1.avsc'
        )
        return avro.schema.parse(open(schema_path).read())

    @cached_property
    def avro_string_writer(self):
        return AvroStringWriter(self.schema)

    def pack(self, message):
        return self.avro_string_writer.encode(message.get_avro_repr())


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
