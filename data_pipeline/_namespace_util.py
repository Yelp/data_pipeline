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
        namespace_name=None,
        environment=None,
        cluster=None,
        database=None,
        transformers=[]
    ):
        """Creates object, if given a namespace_name, will parse to create the object.
        Otherwise, will use raw field assignment.

        Params:
            namespace_name (str): If given, will automatically choose the namespace fields by parsing the name.
                            Rough format: ENVIRONMENT?.CLUSTER.DATABASE(.TRANSFORMER)*

            environment (str): The namespace's environment
            cluster (str): The namespace's cluster (required if building from raw fields)
            database (str): The namespace's database (required if building from raw fields)
            transformers (listof(str)): The transformations applied to the namespace
        """
        if namespace_name is None and (
            cluster is None or
            database is None
        ):
            raise ValueError("namespace_name or all of cluster, database must be defined")
        if namespace_name is not None and (
            environment or
            cluster or
            database or
            transformers
        ):
            raise ValueError(
                "cannot create namespace by parsing name and raw field assignment simultaneously"
            )
        if namespace_name:
            self._create_from_namespace_name(namespace_name)
            self.was_parsed = True
        else:
            self.environment = environment
            self.cluster = cluster
            self.database = database
            self.transformers = transformers
            self.was_parsed = False

    def _build_from_sections(self, sections, environment_exists):
        if environment_exists:
            cluster_pos = 1
            self.environment = sections[0]
        else:
            cluster_pos = 0
            self.environment = None
        self.cluster = sections[cluster_pos]
        self.database = sections[cluster_pos + 1]
        self.transformers = sections[cluster_pos + 2:]

    def _create_from_namespace_name(self, namespace_name):
        self._validate_namespace_name(namespace_name)
        sections = namespace_name.split('.')
        self._build_from_sections(
            sections,
            self._is_first_section_an_environment(sections)
        )

    def _is_first_section_an_environment(self, sections):
        """Determines whether or not the first section is an environment or cluster definition"""
        return (
            sections[0] in self.ENVIRONMENTS and
            # ex: main.database
            len(sections) >= 3
        )

    def _is_first_section_an_environment_with_guarantees(
        self,
        sections,
        environment=None,
        cluster=None,
        database=None,
        transformers=[]
    ):
        return (
            self._is_first_section_an_environment(sections) and
            (cluster is not None or cluster == sections[1]) and
            (database is not None or database == sections[2]) and
            (set(transformers).issubset(set(sections[3:])))
        ) or sections[0] == environment

    def _validate_namespace_name(self, namespace_name):
        if re.match("^[a-zA-Z0-9_\.-]+$", namespace_name) is None:
            raise ValueError("namespace_name must contain only alphanumeric characters and -, _, .")
        if len(namespace_name.split('.')) < 2:
            raise ValueError("not enough sections to split namespace_name to extract information")

    def _validate_guarantees_satisfied(
        self,
        environment=None,
        cluster=None,
        database=None,
        transformers=[]
    ):
        if (
            (environment is not None and environment != self.environment) or
            (cluster is not None and cluster != self.cluster) or
            (database is not None and database != self.database) or
            not set(transformers).issubset(set(self.transformers))
        ):
            raise ValueError("Guarantees are impossible to rectify with existing namespace")

    def apply_guarantees(
        self,
        environment=None,
        cluster=None,
        database=None,
        transformers=[]
    ):
        """For namespaces created by parsing names, it's possible that we want to modify how they are parsed based on
        something we know about the namespace in question to mitigate possible errors.
        e.g. for main.database.transformer where main is a cluster, normal parsing would treat it as an environment.

        Anything defined by the user will be forced on the name as true to re-parse the namespace

        NOTE: Anything in transformers will be guaranteed to be a subset of the end-transformers
        """
        if not self.was_parsed:
            raise ValueError("Can only apply guarantees on namespaces created by parsing")
        sections = self.get_name().split('.')
        self._build_from_sections(
            sections,
            self._is_first_section_an_environment_with_guarantees(
                sections,
                environment=environment,
                cluster=cluster,
                database=database,
                transformers=transformers
            )
        )
        self._validate_guarantees_satisfied(
            environment=environment,
            cluster=cluster,
            database=database,
            transformers=transformers
        )
        return self

    def get_name(self):
        output = ""
        if self.environment is not None:
            output += self.environment + "."
        output += self.cluster + "."
        output += self.database
        if self.transformers:
            output += "."
        output += '.'.join(self.transformers)
        return output
