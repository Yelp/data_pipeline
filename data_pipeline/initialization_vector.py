# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

from Crypto.Cipher import AES

from data_pipeline.meta_attribute import MetaAttribute


class InitializationVector(MetaAttribute):
    """This MetaAttribute allows us to encrypt messages using AES in CBC
    mode or other strong encryption algorithms requiring a unique random
    seed for each message to be encrypted. The payload is a 16-byte array
    representing the initialization vector used to encrypt a
    message.
    """

    def __init__(self, schema_id, initialization_vector_array=None):
        if initialization_vector_array is None:
            initialization_vector_array = os.urandom(AES.block_size)
        self._verify_initialization_vector_params(initialization_vector_array)
        self.initialization_vector_array = initialization_vector_array

        super(InitializationVector, self).__init__(
            schema_id=schema_id,
            payload_data=self.initialization_vector_array
        )

    def _verify_initialization_vector_params(self, vector_array):
        if not isinstance(vector_array, bytes) or not len(vector_array) == 16:
            raise TypeError('Initialization Vector must be a 16-byte array')

    def _asdict(self):
        return {
            'schema_id': self.schema_id,
            'payload_data': self.payload.encode('hex')
        }
