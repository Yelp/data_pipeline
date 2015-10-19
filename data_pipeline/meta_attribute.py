# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cached_property import cached_property
from yelp_avro.avro_string_reader import AvroStringReader
from yelp_avro.avro_string_writer import AvroStringWriter

from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class MetaAttribute(object):
    """This is an base class to define a meta attribute. A MetaAttribute is
    embedded in a data pipeline message's meta field. It essentially needs to:
        1. Specify an avro schema to encode its payload.
        2. Register that schema with the schematizer.
        3. Enable creating a MetaAttribute object from kwarg param
        'unpack_from_meta_attr' which is essentially a dict with
        schema_id & payload keys.
    """

    def __init__(self, **kwargs):
        if 'unpack_from_meta_attr' in kwargs:
            self.create_from_meta_attribute_dict(kwargs['unpack_from_meta_attr'])

    @cached_property
    def owner_email(self):
        return self.schematizer.get_schema_by_id(self.schema_id).topic.source.owner_email

    @cached_property
    def source(self):
        return self.schematizer.get_schema_by_id(self.schema_id).topic.source.name

    @cached_property
    def namespace(self):
        return self.schematizer.get_schema_by_id(self.schema_id).topic.source.namespace.name

    @cached_property
    def contains_pii(self):
        return self.schematizer.get_schema_by_id(self.schema_id).topic.contains_pii

    @cached_property
    def base_schema_id(self):
        return self.schematizer.get_schema_by_id(self.schema_id).base_schema_id

    @cached_property
    def avro_schema(self):
        return self.schematizer.get_schema_by_id(self.schema_id).schema_json

    @cached_property
    def schematizer(self):
        return get_schematizer()

    @cached_property
    def schema_id(self):
        return self._register_schema()

    @property
    def avro_repr(self):
        return {
            'schema_id': self.schema_id,
            'payload': self.encoded_payload
        }

    def _register_schema(self):
        schema_info = self.schematizer.register_schema_from_schema_json(
            base_schema_id=self.base_schema_id,
            namespace=self.namespace,
            source=self.source,
            schema_json=self.avro_schema,
            source_owner_email=self.owner_email,
            contains_pii=self.contains_pii

        )
        return schema_info.schema_id

    def _get_decoded_payload(self, encoded_payload):
        return AvroStringReader(self.avro_schema, self.avro_schema).decode(encoded_payload)

    @cached_property
    def encoded_payload(self):
        return AvroStringWriter(self.avro_schema).encode(self.payload)

    def create_from_meta_attribute_dict(self, meta_attr_dict):
        if not isinstance(meta_attr_dict, dict):
            raise TypeError(
                "Can set a MetaAttribute object from a dict containing schema_id (int) and payload(bytes)."
            )
        if not isinstance(meta_attr_dict.get('schema_id'), int):
            raise ValueError(
                "Can set a MetaAttribute object from a dict containing schema_id (int) and payload(bytes)."
            )
        self.schema_id = meta_attr_dict.get('schema_id')
        self.payload = self._get_decoded_payload(meta_attr_dict.get('payload'))
