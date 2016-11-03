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

import re


class DBSourcedNamespace(object):
    """Class that provides namespace information based on a given namespace name for database-source namespaces
    (i.e. ones created/used by replication handler).
    """
    ENVIRONMENTS = ['main', 'dev']

    def __init__(
        self,
        cluster,
        database,
        environment=None,
        suffixes=None
    ):
        """Params:
            cluster (str): The namespace's cluster (required if building from raw fields)
            database (str): The namespace's database (required if building from raw fields)
            environment (optional(str)): The namespace's environment
            suffixes (optional(listof(str))): The suffixes of the namespace (i.e. transformations applied to the namespace)
        """
        self.environment = environment
        self.cluster = cluster
        self.database = database
        self.suffixes = suffixes or []

    @classmethod
    def _build_from_sections(cls, sections, environment_exists):
        if environment_exists:
            cluster_pos = 1
            environment = sections[0]
        else:
            cluster_pos = 0
            environment = None
        cluster = sections[cluster_pos]
        database = sections[cluster_pos + 1]
        suffixes = sections[cluster_pos + 2:]
        return DBSourcedNamespace(cluster, database, environment, suffixes)

    @classmethod
    def create_from_namespace_name(cls, namespace_name):
        sections = namespace_name.split('.')
        cls._validate_sections(sections)
        return cls._build_from_sections(
            sections,
            cls._is_first_section_an_environment(sections)
        )

    @classmethod
    def create_from_namespace_name_with_guarantees(
        cls,
        namespace_name,
        expected_cluster=None,
        expected_database=None,
        expected_environment=None,
        expected_suffixes=None
    ):
        """For namespaces created by parsing names, it's possible that we want to modify how they are parsed based on
        something we know about the namespace in question to mitigate possible errors.
        e.g. for main.database.transformer where main is a cluster, normal parsing would treat it as an environment.

        Anything defined by the user will be forced on the name as true to parse the namespace name

        NOTE: Anything in suffixes will be guaranteed to be a subset of the end-suffixes
        """
        sections = namespace_name.split('.')
        cls._validate_sections(sections)
        namespace = cls._build_from_sections(
            sections,
            cls._is_first_section_an_environment_with_guarantees(
                sections,
                expected_cluster=expected_cluster,
                expected_database=expected_database,
                expected_environment=expected_environment,
                expected_suffixes=expected_suffixes
            )
        )
        cls.assert_expectations_satisfied(
            namespace,
            expected_cluster=expected_cluster,
            expected_database=expected_database,
            expected_environment=expected_environment,
            expected_suffixes=expected_suffixes
        )
        return namespace

    @classmethod
    def _is_first_section_an_environment(cls, sections):
        """Determines whether or not the first section is an environment or cluster definition"""
        return (
            sections[0] in cls.ENVIRONMENTS and
            # ex: main.database
            len(sections) >= 3
        )

    @classmethod
    def _is_first_section_an_environment_with_guarantees(
        cls,
        sections,
        expected_cluster=None,
        expected_database=None,
        expected_environment=None,
        expected_suffixes=None
    ):
        # We need to use all of our guarantees to determine if first section is environment
        # since we need all of these rules followed to maximally satisfy our guarantees
        # i.e.: We may only receive the suffixes ['transformer'] on a namespace of main.database.transformer,
        #       By making sure expected suffixes is followed,
        #       we will properly decide that main is not an environment but a cluster name.
        return sections[0] == expected_environment or (
            cls._is_first_section_an_environment(sections) and
            (not expected_cluster or expected_cluster == sections[1]) and
            (not expected_database or expected_database == sections[2]) and
            (not expected_suffixes or set(expected_suffixes).issubset(set(sections[3:])))
        )

    @classmethod
    def _validate_sections(cls, sections):
        if len(sections) < 2:
            raise ValueError(
                "not enough sections to split namespace_name to extract information "
                "(we need at least 2 to extract a cluster and database)"
            )
        for section in sections:
            if not re.match("^[_-]*[a-zA-Z0-9][a-zA-Z0-9_-]*$", section):
                raise ValueError(
                    "namespace_name section must contain at least one alphanumeric character and may include - or _"
                )

    @classmethod
    def assert_expectations_satisfied(
        cls,
        namespace,
        expected_cluster=None,
        expected_database=None,
        expected_environment=None,
        expected_suffixes=None
    ):
        if (
            (expected_environment and expected_environment != namespace.environment) or
            (expected_cluster and expected_cluster != namespace.cluster) or
            (expected_database and expected_database != namespace.database) or
            (expected_suffixes and not set(expected_suffixes).issubset(set(namespace.suffixes)))
        ):
            raise ValueError("Guarantees are impossible to rectify with existing namespace")

    def get_name(self):
        sections = []
        if self.environment:
            sections.append(self.environment)
        sections.append(self.cluster)
        sections.append(self.database)
        sections += self.suffixes
        return '.'.join(sections)
