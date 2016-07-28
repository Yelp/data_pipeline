# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson

from data_pipeline.tools.introspector.list_command.base_command import _BaseListCommand


class NamespacesListCommand(_BaseListCommand):

    list_type = 'namespaces'
    fields = [
        'name', 'namespace_id',
        'active_topic_count', 'active_source_count'
    ]

    @classmethod
    def add_parser(cls, subparsers):
        list_command_parser = subparsers.add_parser(
            "namespaces",
            description=cls.get_description(),
            add_help=False
        )

        list_command_parser.add_argument(
            '--active-namespaces',
            default=False,
            action='store_true',
            help=(
                'If set, this command will also return information about active '
                'sources and topics within each namespace. '
                'This is a time expensive operation.'
            )
        )

        cls.add_base_arguments(list_command_parser)

        list_command_parser.set_defaults(
            command=lambda args:
                cls("data_pipeline_introspector_list_namespaces").run(
                    args,
                    list_command_parser
                )
        )

    def run(self, args, parser):
        self.process_args(args, parser)
        print simplejson.dumps(self.list_namespaces(
            sort_by=self.sort_by,
            descending_order=self.descending_order,
            active_namespaces=args.active_namespaces
        ))
