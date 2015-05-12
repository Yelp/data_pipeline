# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import cStringIO
import os

import avro.io
import avro.schema
from cached_property import cached_property

from data_pipeline.message_type import MessageType


class Envelope(object):
    """Envelope used to encode and identify a message for transport.

    Envelope instances are meant to be long-lived and used to encode multiple
    messages.

    Example:
        >>> from data_pipeline.message import Message
        >>> message = Message(str('topic'), 1, bytes("FAKE MESSAGE"), MessageType.create)
        >>> envelope = Envelope()
        >>> packed_message = envelope.pack(message)
        >>> isinstance(packed_message, bytes)
        True
        >>> unpacked = envelope.unpack(packed_message)
        >>> unpacked['message_type']
        u'create'
        >>> unpacked['schema_id']
        1
        >>> unpacked['payload']
        'FAKE MESSAGE'
    """
    @cached_property
    def _schema(self):
        # Keeping this as an instance method because of issues with sharing
        # this data across processes.
        schema_path = os.path.join(
            os.path.dirname(__file__),
            'schemas/envelope_v1.avsc'
        )
        return avro.schema.parse(open(schema_path).read())

    @cached_property
    def _avro_string_writer(self):
        return _AvroStringWriter(self._schema)

    @cached_property
    def _avro_string_reader(self):
        return _AvroStringReader(self._schema, self._schema)

    def pack(self, message):
        """Packs a message for transport as described in y/cep342.

        Use :func:`unpack` to decode the packed message.

        Args:
            message (data_pipeline.message.Message): The message to pack

        Returns:
            bytes: Avro byte string prepended by magic envelope version byte
        """
        # The initial "magic byte" is currently unused, but is meant to specify
        # the envelope schema version.  see y/cep342 for details.  In other
        # words, the version number of the current schema is the null byte.  In
        # the event we need to add additional envelope versions, we'll use this
        # byte to identify it.
        return bytes(0) + self._avro_string_writer.encode(
            self._get_avro_repr(message)
        )

    def unpack(self, packed_message):
        """Decodes a message packed with :func:`pack`.

        Warning:
            The public API for this function may change to return
            :class:`data_pipeline.message.Message` instances.

        Args:
            packed_message (bytes): The previously packed message

        Returns:
            dict: A dictionary with the decoded Avro representation.
        """
        # The initial "magic byte" is ignored, see the comment in `pack`.
        return self._avro_string_reader.decode(packed_message[1:])

    def _get_avro_repr(self, message):
        """Prepare the message for packing

        Returns:
            dict: Dictionary that can be packed by
                :func:`data_pipeline.envelope.Envelope.pack`.
        """
        if message.message_type == MessageType.update:
            # TODO: This needs to make sure the previous payload is actually
            # set.
            raise NotImplementedError

        return {
            'uuid': message.uuid,
            'message_type': message.message_type.name,
            'schema_id': message.schema_id,
            'payload': message.payload,
            'timestamp': message.timestamp
        }


class _AvroStringWriter(object):
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


class _AvroStringReader(object):
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
