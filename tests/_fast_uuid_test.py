# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline._fast_uuid import _DefaultUUID
from data_pipeline._fast_uuid import _LibUUID


class TestFastUUID(object):

    def test_uuid1(self, fast_uuid):
        assert self._is_valid_uuid(fast_uuid.uuid1())

    def test_uuid1_does_not_repeat(self, fast_uuid):
        assert fast_uuid.uuid1() != fast_uuid.uuid1()

    def test_uuid4(self, fast_uuid):
        assert self._is_valid_uuid(fast_uuid.uuid1())

    def test_uuid4_does_not_repeat(self, fast_uuid):
        assert fast_uuid.uuid4() != fast_uuid.uuid4()

    def _is_valid_uuid(self, uuid_val):
        return isinstance(uuid_val, str) and len(uuid_val) == 16

    def test_use_libuuid_when_available(self, fast_uuid, libuuid_available):
        fast_uuid.uuid1()
        if libuuid_available:
            assert isinstance(fast_uuid._uuid_in_use, _LibUUID)
        else:
            assert isinstance(fast_uuid._uuid_in_use, _DefaultUUID)
