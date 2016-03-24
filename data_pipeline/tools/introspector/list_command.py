# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.base import IntrospectorBatch

class ListCommand(IntrospectorBatch):
    @classmethod
    def add_parser(cls, subparsers):
        list_command_parser = subparsers.add_parser(
            "list",
            description="List the specified items, with a row for each item.",
            add_help=False
        )

        list_command_parser.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show help messages and exit."
        )

        list_command_parser.add_argument(
            "type",
            type=str,
            choices=("topics", "sources", "namespaces"),
            help="What type of object you want to list. "
            "Note: It is required to use the --filter argument for topic lists"
        )

        list_command_parser.add_argument(
            "-s",
            "--sort-by",
            type=str,
            default=None,
            help="Sort the listing by a particular field of the object."
        )

        list_command_parser.add_argument(
            "--namespace-name-filter",
            type=str,
            default=None,
            help="Filter by namespace, using the namespace's name. "
                 "Note: Cannot filter list namespaces by a particular namespace (will be ignored). "
                 "Will be overrided if filtering by source id"
        )

        list_command_parser.add_argument(
            "--namespace-id-filter",
            type=int,
            default=None,
            help="Filter by namespace, using the namespace's id. "
                 "Note: Cannot filter list namespaces by a particular namespace (will be ignored). "
                 "Will be overrided if filtering by source id"
        )

        list_command_parser.add_argument(
            "--source-name-filter",
            type=str,
            default=None,
            help="Filter by source, using the source's name. "
                 "Note: Can only filter topics by sources (will otherwise be ignored). "
                 "Will be overrided if filtering by source id"
        )

        list_command_parser.add_argument(
            "--source-id-filter",
            type=int,
            default=None,
            help="Filter by source, using the source's id. "
                 "Note: Can only filter topics by sources (will otherwise be ignored). "
                 "Will override all other filters"
        )
        list_command_parser.set_defaults(command=cls().run)

    def run(self, args):
        print "This is the list command"


