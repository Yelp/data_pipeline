# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson as json
from Crypto.Cipher import AES

from data_pipeline.initialization_vector import InitializationVector
from data_pipeline.message import Message


class EncryptionHelper(object):
    """The EncryptionHelper provides helper methods for encrypting PII
    in the data pipeline, given a key and message.

    Args:
      producer_position_callback (function): The key to be used in the encryption
      message (data_pipeline.Message): The message with a payload to be encrypted
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
        if not isinstance(message, Message):
            raise TypeError("Message should be a Data Pipeline message")
        self._message = message

    def __init__(self, key=None, message=None):
        self.key = key
        self.message = message

    def _get_initialization_vector(self, message):
        meta = message.meta
        if meta is not None:
            # list comprehension is required to retrieve
            # the InitializationVector object, since
            # message.meta is an unordered list
            vectors = [m for m in meta if isinstance(m, InitializationVector)]
            return vectors.pop().payload

    def encrypt_message_with_pii(self):
        """Encrypt message with key on machine, using AES.
         Mutates the message to have an encrypted payload"""
        return self._encrypt_message_using_pycrypto(self.key, self.message)

    def _encrypt_message_using_pycrypto(self, key, message, encryption_algorithm=None):
        payload = message.payload or message.payload_data
        # eventually we should allow for multiple
        # encryption methods, but for now we assume AES 
        encrypter = AES.new(
            key,
            AES.MODE_CBC,
            self._get_initialization_vector(self.message)
        )
        if isinstance(payload, dict):
            payload = self._pad_payload(json.dumps(payload))
            new_payload = encrypter.encrypt(payload)
            message.payload = new_payload
        else:
            payload = self._pad_payload(payload)
            new_payload = encrypter.encrypt(payload)
            message.payload = new_payload
        return True

    def _pad_payload(self, payload):
        length = 16 - (len(payload) % 16)
        return payload + chr(length) * length

    def decrypt_payload(self, initialization_vector=None):
        if initialization_vector is None:
            initialization_vector = self._get_initialization_vector(self.message)
        decrypter = AES.new(
            self.key, 
            AES.MODE_CBC, 
            initialization_vector
        )
        data = decrypter.decrypt(self.message.payload)
        return self._unpad(data)

    def _unpad(self, s):
        return s[:-ord(s[len(s) - 1:])]
