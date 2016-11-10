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

import pytest

from data_pipeline._encryption_helper import _AVSCStore
from data_pipeline._encryption_helper import initialization_vector_info
from data_pipeline.initialization_vector import get_initialization_vector
from data_pipeline.meta_attribute import MetaAttribute


@pytest.mark.usefixtures('containers')
class TestInitializationVector(object):

    def test_create_vector_fails_with_bad_arg_values(self):
        schema_id = _AVSCStore().get_schema_id(initialization_vector_info)
        invalid_vector_payload_data = bytes(10)

        with pytest.raises(TypeError):
            get_initialization_vector(
                schema_id,
                invalid_vector_payload_data
            )

    def test_initialization_vector_creation(self):
        schema_id = _AVSCStore().get_schema_id(initialization_vector_info)
        vector_payload_data = b'0000000000000000'

        for _payload_data in [vector_payload_data, None]:
            initialization_vector = get_initialization_vector(
                schema_id,
                _payload_data
            )
            assert isinstance(initialization_vector, MetaAttribute)
            assert isinstance(initialization_vector.avro_repr['payload'], bytes)
