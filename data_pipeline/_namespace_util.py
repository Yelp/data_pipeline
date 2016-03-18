# -*- coding: utf-8 -*-
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
        transformers=None
    ):
        """Params:
            cluster (str): The namespace's cluster (required if building from raw fields)
            database (str): The namespace's database (required if building from raw fields)
            environment (optional(str)): The namespace's environment
            transformers (optional(listof(str))): The transformations applied to the namespace
        """
        self.environment = environment
        self.cluster = cluster
        self.database = database
        self.transformers = transformers if transformers else []

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
        transformers = sections[cluster_pos + 2:]
        return DBSourcedNamespace(cluster, database, environment, transformers)

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
        expected_transformers=None
    ):
        """For namespaces created by parsing names, it's possible that we want to modify how they are parsed based on
        something we know about the namespace in question to mitigate possible errors.
        e.g. for main.database.transformer where main is a cluster, normal parsing would treat it as an environment.

        Anything defined by the user will be forced on the name as true to parse the namespace name

        NOTE: Anything in transformers will be guaranteed to be a subset of the end-transformers
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
                expected_transformers=expected_transformers
            )
        )
        namespace._assert_guarantees_satisfied(
            expected_cluster=expected_cluster,
            expected_database=expected_database,
            expected_environment=expected_environment,
            expected_transformers=expected_transformers
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
        expected_transformers=None
    ):
        return (
            cls._is_first_section_an_environment(sections) and
            (not expected_cluster or expected_cluster == sections[1]) and
            (not expected_database or expected_database == sections[2]) and
            (not expected_transformers or set(expected_transformers).issubset(set(sections[3:])))
        ) or sections[0] == expected_environment

    @classmethod
    def _validate_sections(cls, sections):
        for section in sections:
            if not re.match("^[_-]*[a-zA-Z0-9][a-zA-Z0-9_-]*$", section):
                raise ValueError("namespace_name section must contain at least one alphanumeric character and may include - or _")
        if len(sections) < 2:
            raise ValueError(
                "not enough sections to split namespace_name to extract information "
                "(we need at least 2 to extract a cluster and database)"
            )

    def _assert_guarantees_satisfied(
        self,
        expected_cluster=None,
        expected_database=None,
        expected_environment=None,
        expected_transformers=None
    ):
        if (
            (expected_environment and expected_environment != self.environment) or
            (expected_cluster and expected_cluster != self.cluster) or
            (expected_database and expected_database != self.database) or
            (expected_transformers and not set(expected_transformers).issubset(set(self.transformers)))
        ):
            raise ValueError("Guarantees are impossible to rectify with existing namespace")

    def get_name(self):
        output = ""
        if self.environment:
            output += self.environment + "."
        output += self.cluster + "."
        output += self.database
        if self.transformers:
            output += "."
        output += '.'.join(self.transformers)
        return output
