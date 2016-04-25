# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson

from data_pipeline.tools.introspector.base import IntrospectorCommand


class InfoCommand(IntrospectorCommand):
    @classmethod
    def add_parser(cls, subparsers):
        info_command_parser = subparsers.add_parser(
            "info",
            description="Get information on a specific data pipeline item.",
            add_help=False
        )

        cls.add_base_arguments(info_command_parser)

        info_command_parser.add_argument(
            "info_type",
            type=str,
            choices=("topic", "source", "namespace"),
            help="What type of object you want to get information on."
        )

        info_command_parser.add_argument(
            "identifier",
            type=str,
            help="The identifier of the data in question. "
                 "For sources, use format \"SOURCE_NAME|NAMESPACE_NAME\", or the source id. "
                 "For topics and namespaces, use the names of the data types. "
        )

        info_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_info").run(
                args,
                info_command_parser
            )
        )

    def process_args(self, args, parser):
        super(InfoCommand, self).process_args(args, parser)
        self.info_type = args.info_type
        self.identifier = args.identifier
        if self.info_type == "source":
            self.source_name = None
            self.namespace_name = None
            self.source_id = None
            if self.identifier.isdigit():
                self.source_id = int(self.identifier)
            elif len(self.identifier.split("|")) == 2:
                self.source_name = self.identifier.split("|")[0]
                self.namespace_name = self.identifier.split("|")[1]
            else:
                raise parser.error(
                    "Source identifier must be an integer id or in format \"SOURCE_NAME|NAMESPACE_NAME\""
                )

    def run(self, args, parser):
        self.process_args(args, parser)
        if self.info_type == "topic":
            print simplejson.dumps(
                self.info_topic(self.identifier)
            )
        elif self.info_type == "source":
            print simplejson.dumps(
                self.info_source(
                    source_id=self.source_id,
                    source_name=self.source_name,
                    namespace_name=self.namespace_name
                )
            )
        else:
            print simplejson.dumps(
                self.info_namespace(self.identifier)
            )
