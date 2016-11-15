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

        info_command_parser.add_argument(
            '--active-namespaces',
            default=False,
            action='store_true',
            help=(
                'If set, this command will also return information about active '
                'sources and topics for this namespace. '
                'This is a time expensive operation.'
            )
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

    def info_namespace(self, name, active_namespaces=False):
        namespaces = self.schematizer.get_namespaces()
        info_namespace = None
        for namespace in namespaces:
            if namespace.name == name:
                info_namespace = namespace
                break
        if info_namespace:
            namespace = IntrospectorNamespace(
                namespace,
                active_namespaces=(self.active_namespaces if active_namespaces else None)
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
            self.info_namespace(
                self.namespace_name,
                active_namespaces=args.active_namespaces
            )
        )
