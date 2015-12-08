# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from Crypto.Cipher import AES

from data_pipeline.config import get_config
from data_pipeline.initialization_vector import InitializationVector


class EncryptionHelper(object):
    """The EncryptionHelper provides helper methods for encrypting PII
    in the data pipeline, given a key and message.

    Args:
      key (string): The key to be used in the encryption
      message (data_pipeline.message.Message): The message with a payload to be encrypted
    """

    @property
    def key(self):
        return self._key

    @property
    def message(self):
        return self._message

    def __init__(self, message=None):
        self._message = message
        self.key_location = get_config().key_location + 'key-{}.key'
        self._key = self._retrieve_key(self._get_key_id(self.message))

    def _get_key_id(self, message):
        """returns the key number to use when
        encrypting/decrypting pii, allowing for
        key rotation. encryption_type must be
        of the form 'Algorithm_name-{key_id}'"""
        encryption_type = message.encryption_type
        if encryption_type is not None:
            return encryption_type.split('-')[-1]
        else:
            raise ValueError(
                "Encryption type should be set."
            )

    def _retrieve_key(self, key_id):
        with open(self.key_location.format(key_id), 'r') as f:
            return f.read(AES.block_size)

    def _append_initialization_vector(self):
        iv = self.message.get_meta_attr_by_type(self.message.meta, 'initialization_vector')
        if iv is None:
            if self.message.meta is None:
                self.message.meta = []
            iv = InitializationVector()
            self.message.meta.append(iv)

    def encrypt_message_with_pii(self, payload):
        """Encrypt message with key on machine, using AES."""
        self._append_initialization_vector()
        return self._encrypt_message_using_pycrypto(self.key, payload)

    def _encrypt_message_using_pycrypto(self, key, payload, encryption_algorithm=None):
        # eventually we should allow for multiple
        # encryption methods, but for now we assume AES
        encrypter = AES.new(
            key,
            AES.MODE_CBC,
            self.message.get_meta_attr_by_type(self.message.meta, 'initialization_vector').payload
        )
        payload = self._pad_payload(payload)
        return encrypter.encrypt(payload)

    def _pad_payload(self, payload):
        """payloads must be have length equal to
        a multiple of 16 in order to be encrypted
        by AES's CBC algorithm, because it uses
        block chaining. This method adds a chr equal to the
        length needed in bytes, bytes times,
        to the end of payload before encrypting it,
        and the _unpad method removes those bytes"""

        length = 16 - (len(payload) % 16)
        return payload + chr(length) * length

    def decrypt_payload(self, decoded_payload):
        initialization_vector = self.message.get_meta_attr_by_type(
            self.message.meta,
            'initialization_vector'
        )
        if initialization_vector is None:
            raise TypeError("InitializationVector must not be None")
        decrypter = AES.new(
            self.key,
            AES.MODE_CBC,
            initialization_vector.payload
        )
        data = decrypter.decrypt(decoded_payload)
        return self._unpad(data)

    def _unpad(self, s):
        return s[:-ord(s[len(s) - 1:])]
