# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.base import IntrospectorBatch

class RegisterCommand(IntrospectorBatch):
    @classmethod
    def add_parser(cls, subparsers):
        register_command_parser = subparsers.add_parser(
            "register",
            description="Register the given schema to the schematizer. "
                        "It is required to use either --avro-schema or "
                        "--mysql-create-table (If both are given --avro-schema will be used)",
            add_help=False
        )

        register_command_parser.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show help messages and exit."
        )

        register_command_parser.add_argument(
            "--mysql-create-table",
            type=str,
            help="The mysql statement of creating new table"
        )

        register_command_parser.add_argument(
            "--mysql-old-create-table",
            type=str,
            help="The mysql statement of creating old table. "
                 "Can only be used with --mysql-create-table."
        )

        register_command_parser.add_argument(
            "--mysql-alter-table",
            type=str,
            help="The mysql statement of altering table schema. "
                 "Can only be used with --mysql-create-table."
        )

        register_command_parser.add_argument(
            "--avro-schema",
            type=str,
            help="The json of the avro schema."
        )

        register_command_parser.add_argument(
            "--base-schema-id",
            type=int,
            help="The id of the original schema the new avro schema was built upon."
                 "Can only be used with --avro-schema"
        )

        register_command_parser.add_argument(
            "--source",
            type=str,
            required=True,
            help="The source the new schema will be registered to."
        )

        register_command_parser.add_argument(
            "--namespace",
            type=str,
            required=True,
            help="The namespace the new schema will be registered to."
        )

        register_command_parser.add_argument(
            "--source_owner_email",
            type=str,
            required=True,
            help="The email of the owner of the given source."
        )

        register_command_parser.add_argument(
            "--contains-pii",
            dest="pii",
            type=bool,
            required=True,
            help="Flag indicating if schema could possibly contain pii. More info at y/pii"
        )

        register_command_parser.set_defaults(command=cls().run)

    def run(self, args):
        print "This is the register command"


