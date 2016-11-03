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

import os

from Crypto.Cipher import AES

from data_pipeline.meta_attribute import MetaAttribute


def get_initialization_vector(schema_id, initialization_vector_array=None):
    if initialization_vector_array is None:
        initialization_vector_array = os.urandom(AES.block_size)
    _verify_initialization_vector_params(initialization_vector_array)
    return MetaAttribute(
        schema_id=schema_id,
        payload_data=initialization_vector_array
    )


def _verify_initialization_vector_params(vector_array):
    if not isinstance(vector_array, bytes) or not len(vector_array) == 16:
        raise TypeError('Initialization Vector must be a 16-byte array')
