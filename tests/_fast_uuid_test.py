# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

import data_pipeline._fast_uuid
from data_pipeline._fast_uuid import FastUUID


class TestFastUUID(object):
    @pytest.fixture(params=[True, False])
    def libuuid_available(self, request):
        return request.param

    @pytest.yield_fixture
    def fast_uuid(self, libuuid_available):
        # reset the ffi cache
        FastUUID._ffi = None
        FastUUID._libuuid = None
        FastUUID._libuuid_unavailable = False

        if libuuid_available:
            yield FastUUID()
        else:
            with mock.patch.object(data_pipeline._fast_uuid, 'FFI') as ffi_mock:
                ffi_mock.side_effect = Exception
                yield FastUUID()

    def test_uuid1(self, fast_uuid):
        assert self._is_valid_uuid(fast_uuid.uuid1())

    def test_uuid1_does_not_repeat(self, fast_uuid):
        assert fast_uuid.uuid1() != fast_uuid.uuid1()

    def test_uuid4(self, fast_uuid):
        assert self._is_valid_uuid(fast_uuid.uuid1())

    def test_uuid4_does_not_repeat(self, fast_uuid):
        assert fast_uuid.uuid4() != fast_uuid.uuid4()

    @pytest.mark.usefixtures("fast_uuid")
    def test_libuuid_unavailable(self, libuuid_available):
        # This isn't really good form, but it's worth validating that the ffi
        # creation is actually failing, and marking the lib unavailable where
        # appropriate
        assert FastUUID._libuuid_unavailable != libuuid_available

    def _is_valid_uuid(self, uuid_val):
        return isinstance(uuid_val, str) and len(uuid_val) == 16
