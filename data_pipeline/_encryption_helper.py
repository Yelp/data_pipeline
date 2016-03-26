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

        # Get the key number to use, allowing for key rotation. encryption_type
        # must be of the form 'Algorithm_name-{key_id}'
        _, key_id = self._get_algorithm_and_key_id(encryption_type)

        with open(key_location.format(key_id), 'r') as f:
            return f.read(AES.block_size)

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
            return InitializationVector()
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
