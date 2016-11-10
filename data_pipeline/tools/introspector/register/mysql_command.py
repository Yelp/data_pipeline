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


class RegisterMysqlCommand(_BaseRegisterCommand):
    @classmethod
    def add_parser(cls, subparsers):
        register_mysql_command_parser = subparsers.add_parser(
            "mysql",
            description="Register the given mysql statements "
                        "as schemas to the schematizer.",
            add_help=False
        )

        cls.add_base_arguments(register_mysql_command_parser)

        register_mysql_command_parser.add_argument(
            "--create-table",
            type=str,
            required=True,
            help="The mysql statement of creating new table"
        )

        register_mysql_command_parser.add_argument(
            "--old-create-table",
            type=str,
            default=None,
            help="The mysql statement of creating old table. "
        )

        register_mysql_command_parser.add_argument(
            "--alter-table",
            type=str,
            default=None,
            help="The mysql statement of altering table schema. "
        )

        register_mysql_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_register_mysql").run(
                args, register_mysql_command_parser
            )
        )

    def process_args(self, args, parser):
        super(RegisterMysqlCommand, self).process_args(args, parser)
        self.create_table = args.create_table
        self.old_create_table = args.old_create_table
        self.alter_table = args.alter_table

    def run(self, args, parser):
        self.process_args(args, parser)
        schema = self.schematizer.register_schema_from_mysql_stmts(
            namespace=self.namespace,
            source=self.source_name,
            source_owner_email=self.source_owner_email,
            contains_pii=self.pii,
            new_create_table_stmt=self.create_table,
            old_create_table_stmt=self.old_create_table,
            alter_table_stmt=self.alter_table
        )
        self.print_schema(schema)
