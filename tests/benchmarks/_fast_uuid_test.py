# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest


@pytest.mark.usefixtures(
    "config_containers_connections"
)
@pytest.mark.benchmark
class TestBenchFastUUID(object):

    def test_uuid1(self, benchmark, fast_uuid):

        @benchmark
        def create():
            fast_uuid.uuid1()

    def test_uuid4(self, benchmark, fast_uuid):

        @benchmark
        def create():
            fast_uuid.uuid4()
