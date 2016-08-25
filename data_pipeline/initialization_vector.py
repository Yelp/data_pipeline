# -*- coding: utf-8 -*-
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
