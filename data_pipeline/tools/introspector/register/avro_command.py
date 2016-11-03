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
            "--cluster-type",
            dest="cluster_type",
            default='datapipe',
            help="Kafka cluster type to connect. Defaults to datapipe. "
                 "Currently only 'datapipe' and 'scribe' cluster types are "
                 "supported."
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
        self.cluster_type = args.cluster_type
        self.base_schema_id = args.base_schema_id

    def run(self, args, parser):
        self.process_args(args, parser)
        schema = self.schematizer.register_schema(
            namespace=self.namespace,
            source=self.source_name,
            schema_str=self.avro_schema,
            source_owner_email=self.source_owner_email,
            contains_pii=self.pii,
            cluster_type=self.cluster_type,
            base_schema_id=self.base_schema_id
        )
        self.print_schema(schema)
