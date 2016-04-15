# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.base import IntrospectorBatch


class SchemaCheckCommand(IntrospectorBatch):
    @classmethod
    def add_parser(cls, subparsers):
        schema_check_command_parser = subparsers.add_parser(
            "schema-check",
            description="Checks the compatibility of an avro schema with a namespace"
                        " and source (If given schmea is backward and forward compatible).",
            add_help=False
        )

        cls.add_base_arguments(schema_check_command_parser)

        schema_check_command_parser.add_argument(
            "schema",
            type=str,
            help="The avro schema to check."
        )

        schema_check_command_parser.add_argument(
            "source",
            type=str,
            help="Source id or name of source to check against. If a name is given, "
                 "then --namespace must be provided"
        )

        schema_check_command_parser.add_argument(
            "--namespace",
            required=False,
            type=str,
            default=None,
            help="Namespace name that contains a source of source name given. "
                 "If a source id is given, then this will be ignored."
        )

        schema_check_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_schema_check").run(
                args, schema_check_command_parser
            )
        )

    def process_args(self, args, parser):
        super(SchemaCheckCommand, self).process_args(args, parser)
        self.schema = str(args.schema)
        self.source_id = None
        self.source_name = None
        self.namespace = None
        if args.source.isdigit():
            self.source_id = int(args.source)
            self.source_name, self.namespace = self.retrieve_names_from_source_id(
                self.source_id
            )
            if args.namespace:
                self.log.warning(
                    "Since source id was given, --namespace will be ignored"
                )
        else:
            self.source_name = args.source
            if not args.namespace:
                raise parser.error(
                    "--namespace must be provided when given a source name as source identifier."
                )
            self.namespace = args.namespace

    def retrieve_names_from_source_id(self, source_id):
        """Returns (source_name, namespace_name) of source with given source_id"""
        source = self.schematizer.get_source_by_id(source_id)
        return (source.name, source.namespace.name)

    def is_compatible(self):
        is_compatible = self.schematizer.is_avro_schema_compatible(
            avro_schema=self.schema,
            source_name=self.source_name,
            namespace_name=self.namespace
        )
        return is_compatible

    def run(self, args, parser):
        self.process_args(args, parser)
        print {"is_compatible": self.is_compatible()}
