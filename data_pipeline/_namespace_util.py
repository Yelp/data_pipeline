# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import re


class Namespace(object):
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
        """Creates NamespaceInfo object, if given a namespace_info, will use any other given information
        (if present) to create the namespace info object.

        Params:
        environment (optional(str)): Guaranteed to make this the environment if namespace_name makes it possible
        cluster (optional(str)): Guaranteed to make this the cluster if namespace_name makes it possible
        database (optional(str)): Guaranteed to make this the database if namespace_name makes it possible
        transformers (optional(listof(str))): Guaranteed that the transformers
                                              will include everything in transformers if namespace_name
                                              makes it possible."""
        if namespace_name is None and (
            cluster is None or
            database is None
        ):
            raise ValueError("namespace_name or all of cluster, database must be defined")
        if namespace_name:
            self._create_from_namespace_name(
                namespace_name,
                environment=environment,
                cluster=cluster,
                database=database,
                transformers=transformers
            )

            self._validate_optional_params_satisfied(
                environment=environment,
                cluster=cluster,
                database=database,
                transformers=transformers
            )
        else:
            self.environment = environment
            self.cluster = cluster
            self.database = database
            self.transformers = transformers

    def _create_from_namespace_name(
        self,
        namespace_name,
        environment=None,
        cluster=None,
        database=None,
        transformers=[]
    ):
        self._validate_namespace_name(namespace_name)
        sections = namespace_name.split('.')
        if self._is_first_section_an_environment(
            sections,
            environment=environment,
            cluster=cluster,
            database=database,
            transformers=transformers
        ):
            cluster_pos = 1
            self.environment = sections[0]
        else:
            cluster_pos = 0
            self.environment = None
        self.cluster = sections[cluster_pos]
        self.database = sections[cluster_pos + 1]
        self.transformers = sections[cluster_pos + 2:]

    def _is_first_section_an_environment(
        self,
        sections,
        environment=None,
        cluster=None,
        database=None,
        transformers=[]
    ):
        """Determines whether or not the first section is an environment or cluster definition"""
        return (
            sections[0] in self.ENVIRONMENTS and
            # ex: main.database
            len(sections) >= 3 and
            # ex: main.database.transformers (cluster=main)
            (cluster is None or sections[1] == cluster) and
            # ex: main.database.transformers (database=database)
            (database is None or sections[2] == database) and
            # ex: main.database.tr1.tr2 (transformers=[tr1])
            (set(transformers).issubset(set(sections[3:])))
        ) or sections[0] == environment

    def _validate_namespace_name(self, namespace_name):
        if re.match("^[a-zA-Z0-9_\.-]+$", namespace_name) is None:
            raise ValueError("namespace_name must contain only alphanumeric characters and -, _, .")
        if len(namespace_name.split('.')) < 2:
            raise ValueError("not enough sections to split namespace_name to extract information")

    def _validate_optional_params_satisfied(
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
            raise ValueError("Given namespace_name and optional parameters seem impossible to rectify")

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
