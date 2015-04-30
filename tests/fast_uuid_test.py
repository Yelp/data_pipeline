# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.fast_uuid import _FastUUID


class TestFastUUID(object):
    @pytest.fixture
    def fast_uuid(self):
        return _FastUUID()

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
