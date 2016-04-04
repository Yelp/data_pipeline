# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.base import IntrospectorBatch


class SchemaCheckCommand(IntrospectorBatch):
    @classmethod
    def add_parser(cls, subparsers):
        schema_check_command_parser = subparsers.add_parser(
            "schema-check",
            description="Checks the compatibility of two avro schemas "
                        "(i.e. if schema2 will require a new topic).",
            add_help=False
        )

        schema_check_command_parser.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show help messages and exit."
        )

        schema_check_command_parser.add_argument(
            "schema1",
            type=str
        )

        schema_check_command_parser.add_argument(
            "schema2",
            type=str
        )

        schema_check_command_parser.set_defaults(command=cls("data_pipeline_instropsector_schema_check").run)

    def run(self, args):
        print "This is the schema-check command"
