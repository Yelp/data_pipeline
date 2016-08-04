# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cached_property import cached_property

from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class MetaAttribute(object):
    """This is an base class to define a meta attribute. It serves 2 purposes:
    1. Create MetaAttributes: To define a new MetaAttribute, you need to
    inherit from this class and:
        a. Override the avro schema cached property to encode its payload.
        b. Override the cached properties required to register the schema like
        namespace, source, owner_email, etc.
        c. Define a payload which is serializable by the schema specified.

    2. Expose the meta attributes in a data pipeline message as a list of
    MetaAttribute objects. The Message takes care of creating a MetaAttribute
    object by specifying schema_id and encoded payload args in the constructor.
    It populates all the fields like payload, namespace, source, owner_email,
    etc using that information. In this case these fields once set should be
    treated as immutable fields and should not be directly changed.
    """

    def __init__(self, schema_id=None, encoded_payload=None):
        if schema_id and encoded_payload:
            self._validate_and_set_schema_id(schema_id)
            self._validate_and_set_payload(encoded_payload)
        elif schema_id or encoded_payload:
            raise ValueError(
                "Should provide either both schema_id and encoded payload "
                "or neither of them"
            )

    def _validate_and_set_schema_id(self, schema_id):
        if not isinstance(schema_id, int):
            raise TypeError("Schema_id should be an integer.")
        self.schema_id = schema_id

    def _validate_and_set_payload(self, encoded_payload):
        if not isinstance(encoded_payload, bytes):
            raise TypeError("Encoded payload must be bytes type.")
        self.payload = self._get_decoded_payload(encoded_payload)

    @cached_property
    def _schematizer(self):
        return get_schematizer()

    @cached_property
    def _avro_schema_obj(self):
        return self._schematizer.get_schema_by_id(self.schema_id)

    @cached_property
    def owner_email(self):
        return self._avro_schema_obj.topic.source.owner_email

    @cached_property
    def source(self):
        return self._avro_schema_obj.topic.source.name

    @cached_property
    def namespace(self):
        return self._avro_schema_obj.topic.source.namespace.name

    @cached_property
    def contains_pii(self):
        return self._avro_schema_obj.topic.contains_pii

    @cached_property
    def avro_schema(self):
        return self._avro_schema_obj.schema_json

    @cached_property
    def schema_id(self):
        avro_schema_obj = self._schematizer.get_schema_by_schema_json(
            self.avro_schema
        )
        if avro_schema_obj:
            return avro_schema_obj.schema_id
        return self._register_schema()

    def _register_schema(self):
        schema_info = self._schematizer.register_schema_from_schema_json(
            namespace=self.namespace,
            source=self.source,
            schema_json=self.avro_schema,
            source_owner_email=self.owner_email,
            contains_pii=self.contains_pii
        )
        return schema_info.schema_id

    def _get_decoded_payload(self, encoded_payload):
        reader = _AvroStringStore().get_reader(
            reader_id_key=self.schema_id,
            writer_id_key=self.schema_id
        )
        return reader.decode(
            encoded_message=encoded_payload
        )

    @cached_property
    def encoded_payload(self):
        writer = _AvroStringStore().get_writer(self.schema_id)
        return writer.encode(self.payload)

    @property
    def avro_repr(self):
        return {
            'schema_id': self.schema_id,
            'payload': self.encoded_payload
        }

    def _asdict(self):
        """ Helper method for simplejson encoding in the Tailer
        """
        return {
            'schema_id': self.schema_id,
            'payload': self.payload
        }

    def __repr__(self):
        return '{}'.format(self._asdict())
