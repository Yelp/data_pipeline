# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline._namespace_util import DBSourcedNamespace


class TestDBSourcedtNamespace(object):
    def test_simple(self):
        name = "refresh_primary.yelp"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name(name),
            expected_name=name,
            expected_cluster="refresh_primary",
            expected_database="yelp"
        )

    def test_main_cluster(self):
        name = "main.database"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name(name),
            expected_name=name,
            expected_cluster="main",
            expected_database="database"
        )

    def test_environment(self):
        name = "main.refresh_primary.yelp"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name(name),
            expected_name=name,
            expected_cluster="refresh_primary",
            expected_database="yelp",
            expected_environment="main"
        )

    def test_tranformers(self):
        name = "dev.refresh_primary.yelp.heartbeat.yelp-main_transformed"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name(name),
            expected_name=name,
            expected_cluster="refresh_primary",
            expected_database="yelp",
            expected_environment="dev",
            expected_suffixes=["heartbeat", "yelp-main_transformed"]
        )

    def test_fail_missing(self):
        self._assert_failure("yelp", error_substr="not enough sections")
        self._assert_failure("refresh_primary", error_substr="not enough sections")

    def test_fail_invalid_chars(self):
        self._assert_failure("^refresh_primary.yelp", error_substr="must contain at least")
        self._assert_failure("fadjskl;.fjd", error_substr="must contain at least")
        self._assert_failure("______.______", error_substr="must contain at least")
        self._assert_failure("refresh_primary..yelp", error_substr="must contain at least")

    def test_guarantees(self):
        name = "main.database.transformer"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name_with_guarantees(
                name,
                expected_cluster="main"
            ),
            expected_name=name,
            expected_cluster="main",
            expected_database="database",
            expected_suffixes=["transformer"]
        )

    def test_guarantees_db(self):
        name = "main.database.transformer"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name_with_guarantees(
                name,
                expected_database="database"
            ),
            expected_name=name,
            expected_cluster="main",
            expected_database="database",
            expected_suffixes=["transformer"]
        )

    def test_guarantees_transformer(self):
        name = "main.database.transformer"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name_with_guarantees(
                name,
                expected_suffixes=["transformer"]
            ),
            expected_name=name,
            expected_cluster="main",
            expected_database="database",
            expected_suffixes=["transformer"]
        )

    def test_guarantees_environment(self):
        name = "env.cluster.database"
        self._assert_success(
            actual_namespace=DBSourcedNamespace.create_from_namespace_name_with_guarantees(
                name,
                expected_environment="env"
            ),
            expected_name=name,
            expected_environment="env",
            expected_cluster="cluster",
            expected_database="database"
        )

    def test_fail_impossible(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._assert_failure_with_guarantees(
            name,
            expected_environment="main"
        )

    def test_fail_impossible_suffixes(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._assert_failure_with_guarantees(
            name,
            expected_suffixes=["heartbeat"]
        )

    def test_fail_impossible_double_cluster_env(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._assert_failure_with_guarantees(
            name,
            expected_environment="dev",
            expected_cluster="dev"
        )

    def test_fail_impossible_env_db(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._assert_failure_with_guarantees(
            name,
            expected_environment="dev",
            expected_database="refresh_primary"
        )

    def test_no_name(self):
        self._assert_success(
            actual_namespace=DBSourcedNamespace(
                environment="main",
                cluster="refresh_primary",
                database="yelp"
            ),
            expected_name="main.refresh_primary.yelp",
            expected_environment="main",
            expected_cluster="refresh_primary",
            expected_database="yelp"
        )

    def test_no_name_no_env(self):
        self._assert_success(
            actual_namespace=DBSourcedNamespace(
                cluster="refresh_primary",
                database="yelp",
                suffixes=["heartbeat"]
            ),
            expected_name="refresh_primary.yelp.heartbeat",
            expected_cluster="refresh_primary",
            expected_database="yelp",
            expected_suffixes=["heartbeat"]
        )

    def _assert_failure(self, name, error_substr):
        with pytest.raises(ValueError) as e:
            DBSourcedNamespace.create_from_namespace_name(name)
            assert error_substr in e

    def _assert_failure_with_guarantees(
        self,
        name,
        expected_cluster=None,
        expected_database=None,
        expected_environment=None,
        expected_suffixes=None
    ):
        with pytest.raises(ValueError) as e:
            DBSourcedNamespace.create_from_namespace_name_with_guarantees(
                name,
                expected_environment=expected_environment,
                expected_cluster=expected_cluster,
                expected_database=expected_database,
                expected_suffixes=expected_suffixes
            )
            assert "impossible to rectify" in e

    def _assert_success(
        self,
        actual_namespace,
        expected_name,
        expected_cluster,
        expected_database,
        expected_environment=None,
        expected_suffixes=None
    ):
        if not expected_suffixes:
            expected_suffixes = []
        assert actual_namespace.get_name() == expected_name
        assert actual_namespace.cluster == expected_cluster
        assert actual_namespace.database == expected_database
        assert actual_namespace.environment == expected_environment
        assert actual_namespace.suffixes == expected_suffixes
