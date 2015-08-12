# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class TestSchemaRef(object):

    def test_source_to_ref_map_is_complete(
            self,
            schema_ref,
            good_source_ref,
            bad_source_ref
    ):
        assert schema_ref.get_source_ref('good_source') == good_source_ref
        assert schema_ref.get_source_ref('bad_source') == bad_source_ref
        assert len(schema_ref._source_to_ref_map) == 2

    def test_source_to_ref_map_can_be_empty(self, schema_ref):
        schema_ref.schema_ref = {}
        assert len(schema_ref._source_to_ref_map) == 0

    def test_defaults_are_respected(self, schema_ref, schema_ref_defaults):
        for key, val in schema_ref_defaults.items():
            assert schema_ref.get_source_val('bad_source', key) == val
