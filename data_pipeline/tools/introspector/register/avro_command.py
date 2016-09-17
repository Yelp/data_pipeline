# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.register.base_command import _BaseRegisterCommand


class RegisterAvroCommand(_BaseRegisterCommand):
    @classmethod
    def add_parser(cls, subparsers):
        register_avro_command_parser = subparsers.add_parser(
            "avro",
            description="Register the given avro schema to the schematizer.",
            add_help=False
        )

        cls.add_base_arguments(register_avro_command_parser)

        register_avro_command_parser.add_argument(
            "--avro-schema",
            type=str,
            required=True,
            help="The json of the avro schema."
        )

        register_avro_command_parser.add_argument(
            "--base-schema-id",
            type=int,
            default=None,
            help="The id of the original schema the new avro schema was built upon."
        )

        register_avro_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_register_avro").run(
                args, register_avro_command_parser
            )
        )

    def process_args(self, args, parser):
        super(RegisterAvroCommand, self).process_args(args, parser)
        self.avro_schema = args.avro_schema
        self.base_schema_id = args.base_schema_id

    def run(self, args, parser):
        self.process_args(args, parser)
        schema = self.schematizer.register_schema(
            namespace=self.namespace,
            source=self.source_name,
            schema_str=self.avro_schema,
            source_owner_email=self.source_owner_email,
            contains_pii=self.pii,
            is_log=self.is_log,
            base_schema_id=self.base_schema_id
        )
        self.print_schema(schema)
