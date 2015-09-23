# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cached_property import cached_property

from data_pipeline.message import MetaAttribute
from data_pipeline.schema_cache import get_schema_cache


class BaseMetaAttribute(object):
    """This is an abstract class to define a meta attribute. A MetaAttribute is
    embedded in a data pipeline message's meta field. It essentially needs to:
        1. Specify an avro schema to encode its payload.
        2. Register that schema with the schematizer.
        3. Expose the meta attribute as a MetaAttribute tuple
           (see data_pipeline.message) of (schema_id, payload).
    The packing and unpacking of the payload using the avro schema is done
    by the Envelope.
    """

    @property
    def owner_email(self):
        raise NotImplementedError

    @property
    def source(self):
        raise NotImplementedError

    @property
    def namespace(self):
        raise NotImplementedError

    @property
    def contains_pii(self):
        raise NotImplementedError

    @property
    def payload(self):
        raise NotImplementedError

    @property
    def schema(self):
        raise NotImplementedError

    @property
    def schematizer_client(self):
        return get_schema_cache()

    @cached_property
    def schema_id(self):
        # TODO: Use new schematizer_client methods before pushing this
        schema_info = self.schematizer_client.register_transformed_schema(
            base_schema_id=0,
            namespace=self.namespace,
            source=self.source,
            schema=self.schema,
            owner_email=self.owner_email,
            contains_pii=self.contains_pii

        )
        return schema_info.schema_id

    def get_meta_attribute(self):
        return MetaAttribute(self.schema_id, self.payload)
