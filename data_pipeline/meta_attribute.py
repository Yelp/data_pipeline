# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cached_property import cached_property

from data_pipeline.schema_cache import get_schema_cache


class MetaAttribute(object):

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
        return self.schema_id, self.payload
