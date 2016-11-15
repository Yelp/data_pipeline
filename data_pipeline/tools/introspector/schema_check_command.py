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

from data_pipeline.tools.introspector.base_command import IntrospectorCommand


class SchemaCheckCommand(IntrospectorCommand):
    @classmethod
    def add_parser(cls, subparsers):
        schema_check_command_parser = subparsers.add_parser(
            "schema-check",
            description="Checks the compatibility of an avro schema and all"
                        " given avro_schemas within the given namespace"
                        " and source. Compatibility means that the schema can"
                        " deserialize data serialized by existing schemas within"
                        " all topics and vice-versa.",
            add_help=False
        )

        cls.add_base_arguments(schema_check_command_parser)
        cls.add_source_and_namespace_arguments(schema_check_command_parser)

        schema_check_command_parser.add_argument(
            "schema",
            type=str,
            help="The avro schema to check."
        )

        schema_check_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_schema_check").run(
                args, schema_check_command_parser
            )
        )

    def process_args(self, args, parser):
        super(SchemaCheckCommand, self).process_args(args, parser)
        self.process_source_and_namespace_args(args, parser)
        self.schema = args.schema

    def is_compatible(self):
        is_compatible = self.schematizer.is_avro_schema_compatible(
            avro_schema_str=self.schema,
            source_name=self.source_name,
            namespace_name=self.namespace
        )
        return is_compatible

    def run(self, args, parser):
        self.process_args(args, parser)
        print {"is_compatible": self.is_compatible()}
