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
from collections import namedtuple

import simplejson
from Crypto.Cipher import AES

from data_pipeline.config import get_config
from data_pipeline.helpers.decorators import memoized
from data_pipeline.helpers.singleton import Singleton
from data_pipeline.initialization_vector import get_initialization_vector
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


_AVSCInfo = namedtuple('_AVSCInfo', (
    'id',
    'avsc_file_path',
    'namespace',
    'source',
    'source_owner_email',
    'contains_pii'
))


initialization_vector_info = _AVSCInfo(
    1,
    'data_pipeline/schemas/initialization_vector_v1.avsc',
    'yelp.data_pipeline',
    'initialization_vector',
    'bam+data_pipeline@yelp.com',
    False
)


class _AVSCStore(object):
    """Services/Applications are responsible for registering the
    meta attribute avro schemas and caching them if necessary.
    For the meta attributes such as encryption
    (or initialization_vector) which is added by the clientlib
    internally, this class is then designed to register
    and cache the avro schemas for such meta attributes inside
    the clientlib.

    This may be replcaced with something better in the future. This class
    is not meant to be used outside of this file.
    """

    __metaclass__ = Singleton

    def __init__(self):
        self._schematizer = get_schematizer()
        self._schema_id_cache = {}

    def get_schema_id(self, avro_schema_info):
        key = avro_schema_info.id
        schema_id = self._schema_id_cache.get(key)
        if not schema_id:
            schema_id = self._load_schema(avro_schema_info)
        return schema_id

    def update_schema_cache(self, avro_schema_info, schema_id):
        self._schema_id_cache[avro_schema_info.id] = schema_id

    def _load_schema(self, avro_schema_info):
        schema_info = self._register_schema(avro_schema_info)
        self.update_schema_cache(avro_schema_info, schema_info.schema_id)
        return schema_info.schema_id

    def _load_avro_schema_file(self, avsc_file_path):
        schema_path = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            avsc_file_path
        )
        with open(schema_path, 'r') as f:
            return simplejson.loads(f.read())

    def _register_schema(self, avro_schema_info):
        avro_schema_json = self._load_avro_schema_file(
            avro_schema_info.avsc_file_path
        )
        return self._schematizer.register_schema_from_schema_json(
            namespace=avro_schema_info.namespace,
            source=avro_schema_info.source,
            schema_json=avro_schema_json,
            source_owner_email=avro_schema_info.source_owner_email,
            contains_pii=avro_schema_info.contains_pii
        )


class EncryptionHelper(object):
    """The EncryptionHelper provides helper methods for encrypting message
    payload in the data pipeline.

    Args:
        encryption_type (string): string indicating the encryption algorithm and
            encryption key used to encrypt/decrypt payload.  The format must be
            "{algorithm_name}-{key_id}".  The "key_id" is the file name that
            contains specific encryption key for the specified algorithm.
        encryption_meta (:class:data_pipeline.meta_attribute.MetaAttribute):
            meta attribute that contains necessary information for the given
            encryption type to perform encryption/decryption.  For example, if
            the encryption type is AES algorithm, the meta attribute is then
            the initialization vector meta attribute.

    Remarks:
        This class currently is implemented specifically for AES algorithm,
        although the original design is to support multiple encryption algorithms.
    """

    def __init__(self, encryption_type, encryption_meta=None):
        key_location = get_config().key_location + 'key-{}.key'
        self.key = self._retrieve_key(encryption_type, key_location)
        self.encryption_meta = (
            encryption_meta or
            self.get_encryption_meta_by_encryption_type(encryption_type)
        )

    def _retrieve_key(self, encryption_type, key_location):
        if not encryption_type:
            raise ValueError("Encryption type should be set.")

        # Get the key number to use, allowing for key rotation.
        _, key_id = self._get_algorithm_and_key_id(encryption_type)

        return fetch_encyption_key(key_location.format(key_id))

    @classmethod
    def _get_algorithm_and_key_id(cls, encryption_type):
        # encryption_type must be of the form 'Algorithm_name-{key_id}'
        algorithm, key_id = encryption_type.split('-')
        return algorithm, key_id

    @classmethod
    def get_encryption_meta_by_encryption_type(cls, encryption_type):
        """This function returns the meta attribute for the given encryption type.

        Remarks:
            Currently because only AES algorithm is supported, it returns
            `class:data_pipeline.initialization_vector.InitializationVector` meta
            attribute directly.
        """
        algorithm, _ = cls._get_algorithm_and_key_id(encryption_type)
        if algorithm:
            schema_id = _AVSCStore().get_schema_id(initialization_vector_info)
            return get_initialization_vector(schema_id)
        raise Exception(
            "Encryption algorithm {} is not supported.".format(algorithm)
        )

    def encrypt_payload(self, payload):
        """Encrypt payload with key on machine, using AES."""
        encrypter = AES.new(
            self.key,
            AES.MODE_CBC,
            self.encryption_meta.payload
        )
        payload = self._pad_payload(payload)
        return encrypter.encrypt(payload)

    def _pad_payload(self, payload):
        """payloads must have length equal to a multiple of 16 in order to
        be encrypted by AES's CBC algorithm, because it uses block chaining.
        This method adds a chr equal to the length needed in bytes, bytes times,
        to the end of payload before encrypting it, and the _unpad method
        removes those bytes.
        """
        length = 16 - (len(payload) % 16)
        return payload + chr(length) * length

    def decrypt_payload(self, payload):
        decrypter = AES.new(
            self.key,
            AES.MODE_CBC,
            self.encryption_meta.payload
        )
        data = decrypter.decrypt(payload)
        return self._unpad(data)

    def _unpad(self, payload):
        return payload[:-ord(payload[len(payload) - 1:])]


@memoized
def fetch_encyption_key(file_name):
    with open(file_name, 'r') as f:
        return f.read(AES.block_size)
