# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline._namespace_util import NamespaceInfo

class TestNamespaceInfo(object):
    def test_simple(self):
        self._verify_success(
            name="refresh_primary.yelp",
            cluster="refresh_primary",
            database="yelp"
        )

    def test_environment(self):
        self._verify_success(
            name="main.refresh_primary.yelp",
            cluster="refresh_primary",
            database="yelp",
            environment="main"
        )

    def test_tranformers(self):
        self._verify_success(
            name="dev.refresh_primary.yelp.heartbeat.yelp-main_transformed",
            cluster="refresh_primary",
            database="yelp",
            environment="dev",
            transformers=["heartbeat", "yelp-main_transformed"]
        )

    def test_fail_missing(self):
        self._verify_failure("yelp")
        self._verify_failure("refresh_primary")

    def _verify_failure(self, name):
        with pytest.raises(ValueError):
            NamespaceInfo(name)

    def _verify_success(self, name, cluster, database, environment=None, transformers=[]):
        namespace_info = NamespaceInfo(name)
        assert namespace_info.cluster == cluster
        assert namespace_info.database == database
        assert namespace_info.environment == environment
        assert namespace_info.transformers == transformers

