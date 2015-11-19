# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import avro.io
import avro.schema
from cached_property import cached_property
from yelp_avro.avro_string_reader import AvroStringReader
from yelp_avro.avro_string_writer import AvroStringWriter


class Envelope(object):
    """Envelope used to encode and identify a message for transport.

    Envelope instances are meant to be long-lived and used to encode multiple
    messages.

    Example:
        >>> from data_pipeline.message import CreateMessage
        >>> message = CreateMessage(str('topic'), 1, bytes("FAKE MESSAGE"))
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
        return AvroStringWriter(self._schema)

    @cached_property
    def _avro_string_reader(self):
        return AvroStringReader(self._schema, self._schema)

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
        message.reload_data()
        return bytes(0) + self._avro_string_writer.encode(message.avro_repr)

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

    def pack_keys(self, keys):
        """Encode primary keys in message.

        Args:
            keys (tuple of str): a tuple of primary keys

        Returns:
            bytes: return bytes with encoded keys. All non-alphanumerics are
                escaped.
        """
        escaped_keys = ('\'' + key.replace('\\', '\\\\').replace("'", "\\'") + '\'' for key in keys)
        return '\x1f'.join(escaped_keys).encode('utf-8')
