# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.base import IntrospectorBatch


class InfoCommand(IntrospectorBatch):
    @classmethod
    def add_parser(cls, subparsers):
        info_command_parser = subparsers.add_parser(
            "info",
            description="Get information on a specific data pipeline item.",
            add_help=False
        )

        info_command_parser.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show help messages and exit."
        )

        info_command_parser.add_argument(
            "type",
            type=str,
            choices=("topic", "source", "namespace"),
            help="What type of object you want to get information on."
        )

        info_command_parser.add_argument(
            "--name",
            type=str,
            default=None,
            help="The name of the data in question. "
                 "For sources, use format SOURCE_NAME|NAMESPACE_NAME_OR_ID. "
                 "For sources with namespaces with numeric names, this method is unavailable."
        )

        info_command_parser.add_argument(
            "--id",
            type=int,
            default=None,
            help="The id of the data in question. "
                 "For topics, this method is unavailable. "
                 "Note: --id will override --name if both are used."
        )

        info_command_parser.set_defaults(command=cls("data_pipeline_instropsector_info").run)

    def run(self, args):
        print "This is the list command"
