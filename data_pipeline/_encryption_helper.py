# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from Crypto.Cipher import AES

from data_pipeline.config import get_config
from data_pipeline.initialization_vector import InitializationVector


class EncryptionHelper(object):
    """The EncryptionHelper provides helper methods for encrypting message
    payload in the data pipeline.

    Args:
        encryption_type (string): Encryption algorithm used to encrypt/decrypt
            payload
        initialization_vector: initialization vector used in AES algorithm

    Remarks:
        This class currently is implemented specifically for AES algorithm,
        although the original design is to support multiple encryption algorithms.
    """

    def __init__(self, encryption_type, initialization_vector=None):
        key_location = get_config().key_location + 'key-{}.key'
        self.key = self._retrieve_key(encryption_type, key_location)
        self.initialization_vector = InitializationVector(initialization_vector)

    def _retrieve_key(self, encryption_type, key_location):
        if not encryption_type:
            raise ValueError("Encryption type should be set.")

        # Get the key number to use, allowing for key rotation. encryption_type
        # must be of the form 'Algorithm_name-{key_id}'
        key_id = encryption_type.split('-')[-1]

        with open(key_location.format(key_id), 'r') as f:
            return f.read(AES.block_size)

    @property
    def encryption_meta(self):
        return self.initialization_vector

    @classmethod
    def get_meta_schema_id(cls):
        return InitializationVector().schema_id

    def encrypt_payload(self, payload):
        """Encrypt payload with key on machine, using AES."""
        encrypter = AES.new(
            self.key,
            AES.MODE_CBC,
            self.initialization_vector.payload
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
            self.initialization_vector.payload
        )
        data = decrypter.decrypt(payload)
        return self._unpad(data)

    def _unpad(self, payload):
        return payload[:-ord(payload[len(payload) - 1:])]
