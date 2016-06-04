# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson

from data_pipeline.tools.introspector.base_command import IntrospectorCommand
from data_pipeline.tools.introspector.models import IntrospectorNamespace


class NamespaceInfoCommand(IntrospectorCommand):
    @classmethod
    def add_parser(cls, subparsers):
        info_command_parser = subparsers.add_parser(
            "namespace",
            description="Get information on a specific data pipeline namespace.",
            add_help=False
        )

        cls.add_base_arguments(info_command_parser)

        info_command_parser.add_argument(
            "namespace_name",
            type=str,
            help="Name of namespace to retrieve information on."
        )

        info_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_info_namespace").run(
                args,
                info_command_parser
            )
        )

    def info_namespace(self, name):
        namespaces = self.schematizer.get_namespaces()
        info_namespace = None
        for namespace in namespaces:
            if namespace.name == name:
                info_namespace = namespace
                break
        if info_namespace:
            namespace = IntrospectorNamespace(
                namespace,
                active_namespaces=self.active_namespaces
            ).to_ordered_dict()
            namespace['sources'] = self.list_sources(
                namespace_name=namespace['name']
            )
            return namespace
        else:
            raise ValueError("Given namespace doesn't exist")

    def process_args(self, args, parser):
        super(NamespaceInfoCommand, self).process_args(args, parser)
        self.namespace_name = args.namespace_name

    def run(self, args, parser):
        self.process_args(args, parser)
        print simplejson.dumps(
            self.info_namespace(self.namespace_name)
        )
