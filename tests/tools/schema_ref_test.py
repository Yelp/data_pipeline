# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals


class TestSchemaRef(object):

    def test_source_to_ref_map_is_complete(
        self,
        schema_ref,
        good_source_ref,
        bad_source_ref,
        source
    ):
        assert schema_ref.get_source_ref(source) == good_source_ref
        assert schema_ref.get_source_ref('bad_source') == bad_source_ref
        assert len(schema_ref._source_to_ref_map) == 2

    def test_source_to_ref_map_can_be_empty(self, schema_ref):
        schema_ref.schema_ref = {}
        assert len(schema_ref._source_to_ref_map) == 0

    def test_defaults_are_respected(self, schema_ref, schema_ref_defaults):
        for key, val in schema_ref_defaults.items():
            assert schema_ref.get_source_val('bad_source', key) == val
