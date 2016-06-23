# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

import data_pipeline._fast_uuid
from data_pipeline._fast_uuid import _DefaultUUID
from data_pipeline._fast_uuid import _LibUUID
from data_pipeline._fast_uuid import FastUUID


class TestFastUUID(object):

    @pytest.fixture(params=[True, False])
    def libuuid_available(self, request):
        return request.param

    @pytest.yield_fixture
    def fast_uuid(self, libuuid_available):
        if libuuid_available:
            yield FastUUID()
        else:
            with mock.patch.object(
                data_pipeline._fast_uuid,
                'FFI',
                side_effect=Exception
            ):
                # Save and restore the existing state; this will allow already
                # instantiated FastUUID instances to keep working.
                original_ffi = data_pipeline._fast_uuid._LibUUID._ffi
                data_pipeline._fast_uuid._LibUUID._ffi = None
                try:
                    yield FastUUID()
                finally:
                    data_pipeline._fast_uuid._LibUUID._ffi = original_ffi

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
