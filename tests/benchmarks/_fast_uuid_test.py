# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline._fast_uuid import FastUUID


@pytest.mark.usefixtures(
    "config_containers_connections"
)
@pytest.mark.benchmark
class TestBenchFastUUID(object):

    @pytest.fixture
    def fuuid(self):
        return FastUUID()

    def test_uuid1(self, benchmark, fuuid):

        @benchmark
        def create():
            fuuid.uuid1()

    def test_uuid4(self, benchmark, fuuid):

        @benchmark
        def create():
            fuuid.uuid4()
