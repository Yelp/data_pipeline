# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline._namespace_util import Namespace


class TestNamespace(object):
    def test_simple(self):
        name = "refresh_primary.yelp"
        self._verify_success(
            namespace=Namespace(name),
            name=name,
            cluster="refresh_primary",
            database="yelp"
        )

    def test_main_cluster(self):
        name = "main.database"
        self._verify_success(
            namespace=Namespace(name),
            name=name,
            cluster="main",
            database="database"
        )

    def test_environment(self):
        name = "main.refresh_primary.yelp"
        self._verify_success(
            namespace=Namespace(name),
            name=name,
            cluster="refresh_primary",
            database="yelp",
            environment="main"
        )

    def test_tranformers(self):
        name = "dev.refresh_primary.yelp.heartbeat.yelp-main_transformed"
        self._verify_success(
            namespace=Namespace(name),
            name=name,
            cluster="refresh_primary",
            database="yelp",
            environment="dev",
            transformers=["heartbeat", "yelp-main_transformed"]
        )

    def test_fail_missing(self):
        self._verify_failure("yelp", error_substr="not enough sections")
        self._verify_failure("refresh_primary", error_substr="not enough sections")

    def test_fail_invalid_chars(self):
        self._verify_failure("^refresh_primary.yelp", error_substr="must contain only")
        self._verify_failure("fadjskl;.fjd", error_substr="must contain only")

    def test_optional_params(self):
        name = "main.database.transformer"
        self._verify_success(
            namespace=Namespace(name, cluster="main"),
            name=name,
            cluster="main",
            database="database",
            transformers=["transformer"]
        )

    def test_optional_params_db(self):
        name = "main.database.transformer"
        self._verify_success(
            namespace=Namespace(name, database="database"),
            name=name,
            cluster="main",
            database="database",
            transformers=["transformer"]
        )

    def test_optional_params_transformer(self):
        name = "main.database.transformer"
        self._verify_success(
            namespace=Namespace(name, transformers=["transformer"]),
            name=name,
            cluster="main",
            database="database",
            transformers=["transformer"]
        )

    def test_double_main(self):
        name = "main.main.database.transformer"
        self._verify_success(
            namespace=Namespace(name, cluster="main"),
            name=name,
            environment="main",
            cluster="main",
            database="database",
            transformers=["transformer"]
        )

    def test_double_main_auto(self):
        name = "main.main.database.transformer"
        self._verify_success(
            namespace=Namespace(name),
            name=name,
            environment="main",
            cluster="main",
            database="database",
            transformers=["transformer"]
        )

    def test_double_main_no_env(self):
        name = "main.main.transformer"
        self._verify_success(
            namespace=Namespace(name, database="main"),
            name=name,
            cluster="main",
            database="main",
            transformers=["transformer"]
        )

    def test_double_main_no_env_transformer(self):
        name = "main.main.transformer"
        self._verify_success(
            namespace=Namespace(name, transformers=["transformer"]),
            name=name,
            cluster="main",
            database="main",
            transformers=["transformer"]
        )

    def test_fail_impossible(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._verify_failure(
            name,
            environment="main",
            error_substr="impossible to rectify"
        )

    def test_fail_impossible_transformers(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._verify_failure(
            name,
            transformers=["heartbeat"],
            error_substr="impossible to rectify"
        )

    def test_fail_impossible_double_cluster_env(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._verify_failure(
            name,
            environment="dev",
            cluster="dev",
            error_substr="impossible to rectify"
        )

    def test_fail_impossible_env_db(self):
        name = "dev.refresh_primary.yelp.transformer"
        self._verify_failure(
            name,
            environment="dev",
            database="refresh_primary",
            error_substr="impossible to rectify"
        )

    def test_no_name(self):
        self._verify_success(
            namespace=Namespace(
                environment="main",
                cluster="refresh_primary",
                database="yelp"
            ),
            name="main.refresh_primary.yelp",
            environment="main",
            cluster="refresh_primary",
            database="yelp"
        )

    def test_no_name_no_env(self):
        self._verify_success(
            namespace=Namespace(
                cluster="refresh_primary",
                database="yelp",
                transformers=["heartbeat"]
            ),
            name="refresh_primary.yelp.heartbeat",
            cluster="refresh_primary",
            database="yelp",
            transformers=["heartbeat"]
        )

    def _verify_failure(
        self,
        name,
        environment=None,
        cluster=None,
        database=None,
        transformers=[],
        error_substr=None
    ):
        with pytest.raises(ValueError) as e:
            Namespace(
                name,
                environment=environment,
                cluster=cluster,
                database=database,
                transformers=transformers
            )
            if error_substr is not None:
                assert error_substr in e

    def _verify_success(self, namespace, name, cluster, database, environment=None, transformers=[]):
        assert namespace.get_name() == name
        assert namespace.cluster == cluster
        assert namespace.database == database
        assert namespace.environment == environment
        assert namespace.transformers == transformers
