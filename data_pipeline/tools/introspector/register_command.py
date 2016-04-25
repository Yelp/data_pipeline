# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson

from data_pipeline.tools.introspector.base import IntrospectorCommand


class RegisterCommand(IntrospectorCommand):
    @classmethod
    def add_parser(cls, subparsers):
        register_command_parser = subparsers.add_parser(
            "register",
            description="Register the given schema to the schematizer. "
                        "It is required to use either --avro-schema or "
                        "--mysql-create-table (If both are given --avro-schema will be used)",
            add_help=False
        )

        cls.add_base_arguments(register_command_parser)

        register_command_parser.add_argument(
            "--mysql-create-table",
            type=str,
            default=None,
            help="The mysql statement of creating new table"
        )

        register_command_parser.add_argument(
            "--mysql-old-create-table",
            type=str,
            default=None,
            help="The mysql statement of creating old table. "
                 "Can only be used with --mysql-create-table."
        )

        register_command_parser.add_argument(
            "--mysql-alter-table",
            type=str,
            default=None,
            help="The mysql statement of altering table schema. "
                 "Can only be used with --mysql-create-table."
        )

        register_command_parser.add_argument(
            "--avro-schema",
            type=str,
            default=None,
            help="The json of the avro schema."
        )

        register_command_parser.add_argument(
            "--base-schema-id",
            type=int,
            default=None,
            help="The id of the original schema the new avro schema was built upon."
                 "Can only be used with --avro-schema"
        )

        register_command_parser.add_argument(
            "--source",
            type=str,
            required=True,
            help="Source id or name of source to check against. If a name is given, "
                 "then --namespace must be provided"
        )

        register_command_parser.add_argument(
            "--namespace",
            type=str,
            default=None,
            help="Namespace name that contains a source of source name given. "
                 "If a source id is given, then this will be ignored."
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
            default=False,
            action="store_true",
            help="Flag indicating if schema could possibly contain pii. More info at y/pii"
        )

        register_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_register").run(
                args, register_command_parser
            )
        )

    def process_args(self, args, parser):
        super(RegisterCommand, self).process_args(args, parser)
        self.process_source_and_namespace_args(args, parser)
        fields = [
            'mysql_create_table', 'mysql_old_create_table', 'mysql_alter_table',
            'avro_schema', 'base_schema_id', 'source_owner_email', 'pii'
        ]
        for field in fields:
            setattr(self, field, getattr(args, field))
        if not (self.avro_schema or self.mysql_create_table):
            raise parser.error("--avro-schema or --mysql-create-table is required")
        if self.avro_schema:
            mysql_exclusive_fields = [
                'mysql_create_table', 'mysql_old_create_table', 'mysql_alter_table'
            ]
            mysql_fields_given = [
                field for field in mysql_exclusive_fields if getattr(self, field)
            ]
            if mysql_fields_given:
                self.log.warning(
                    "Given fields: {} will not be used, since --avro_schema was given".format(
                        mysql_fields_given
                    )
                )
        elif self.mysql_create_table and self.base_schema_id:
            self.log.warning(
                "--base-schema-id will not be used in mysql table registration"
            )

    def print_schema_dict(self, schema_dict):
        print simplejson.dumps(schema_dict)

    def run(self, args, parser):
        self.process_args(args, parser)
        if self.avro_schema:
            schema = self.schematizer.register_schema(
                namespace=self.namespace,
                source=self.source_name,
                schema_str=self.avro_schema,
                source_owner_email=self.source_owner_email,
                contains_pii=self.pii,
                base_schema_id=self.base_schema_id
            )
        else:
            schema = self.schematizer.register_schema_from_mysql_stmts(
                namespace=self.namespace,
                source=self.source_name,
                source_owner_email=self.source_owner_email,
                contains_pii=self.pii,
                new_create_table_stmt=self.mysql_create_table,
                old_create_table_stmt=self.mysql_old_create_table,
                alter_table_stmt=self.mysql_alter_table
            )
        schema_dict = self.schema_to_dict(
            schema,
            include_topic_info=True
        )
        self.print_schema_dict(schema_dict)
