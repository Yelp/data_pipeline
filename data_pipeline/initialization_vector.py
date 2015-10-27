# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import simplejson
from cached_property import cached_property

from data_pipeline.meta_attribute import MetaAttribute


class InitializationVector(MetaAttribute):
    """This MetaAttribute allows us to encrypt messages using AES in CBC
    mode or other strong encryption algorithms requiring a unique random
    seed for each message to be encrypted. The payload is a 16-byte array
    representing the initialization vector used to encrypt a
    message. 
    """

    @cached_property
    def owner_email(self):
        return 'bam+data_pipeline@yelp.com'

    @cached_property
    def source(self):
        return 'initialization_vector'

    @cached_property
    def namespace(self):
        return 'yelp.data_pipeline'

    @cached_property
    def contains_pii(self):
        return False

    @cached_property
    def avro_schema(self):
        schema_path = os.path.join(
            os.path.dirname(__file__),
            'schemas/initialization_vector_v1.avsc'
        )
        with open(schema_path, 'r') as f:
            return simplejson.loads(f.read())

    def __init__(self, initialization_vector_array):
        self._verify_init_params(initialization_vector_array)
        self.initialization_vector_array = initialization_vector_array

    def _verify_init_params(self, vector_array):
        if not isinstance(vector_array, bytes) or not len(vector_array)==16:
            raise TypeError('Initialization Vectory must be a 16-byte array')

    @cached_property
    def payload(self):
        return self.initialization_vector_array
        return {
            'initialization_vector': self.initialization_vector_array
        }

    def __eq__(self, other):
        return type(self) is type(other) and \
            self.initialization_vector_array == other.initialization_vector_array

    def __ne__(self, other):
        return not self. __eq__(other)

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return self.initialization_vector_array
