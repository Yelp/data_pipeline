# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re

from Crypto.Cipher import AES

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

    @key.setter
    def key(self, key):
        if len(key) == 0:
            raise TypeError("Key should be non-zero length.")
        self._key = key

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, message):
        self._message = message

    def __init__(self, message=None):
        self.message = message
        self.key_location = '/nail/srv/configs/data_pipeline/key-{}.key'
        self.key = self._retrieve_key(self._get_key_id(self.message))

    def _get_key_id(self, message):
        """returns the key number to use when
        encrypting/decrypting pii, allowing for
        key rotation. encryption_type must be
        of the form 'Algorithm_name-{key_id}'"""
        encryption_type = message.encryption_type
        if encryption_type is not None:
            return encryption_type.split('-')[-1]
        else:
            list_of_files = os.listdir(os.path.dirname(self.key_location))
            pattern = r"key-([0-9]+)"
            max_key_file = max(list_of_files, key=lambda name: int(re.search(pattern, name).group(1)))
            return re.search(pattern, max_key_file).group(1)

    def _retrieve_key(self, key_id):
        with open(self.key_location.format(key_id), 'r') as f:
            return f.read(AES.block_size)

    def encrypt_message_with_pii(self, payload):
        """Encrypt message with key on machine, using AES."""
        return self._encrypt_message_using_pycrypto(self.key, payload)

    def _encrypt_message_using_pycrypto(self, key, payload, encryption_algorithm=None):
        # eventually we should allow for multiple
        # encryption methods, but for now we assume AES
        encrypter = AES.new(
            key,
            AES.MODE_CBC,
            self.message.get_meta_attr_by_type(self.message.meta, InitializationVector).payload
        )
        payload = self._pad_payload(payload)
        return encrypter.encrypt(payload)

    def _pad_payload(self, payload):
        """payloads must be have length equal to
        a multiple of 16 in order to be encrypted
        by AES's CBC algorithm, because it uses
        block chaining. This method adds null bytes
        to the end of payload before encrypting it,
        and the _unpad method removes those null bytes"""

        length = 16 - (len(payload) % 16)
        return payload + chr(length) * length

    def decrypt_payload(self, initialization_vector, decoded_payload):
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
