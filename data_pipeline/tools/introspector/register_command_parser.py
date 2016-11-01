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

from data_pipeline.tools.introspector.register.avro_command import RegisterAvroCommand
from data_pipeline.tools.introspector.register.mysql_command import RegisterMysqlCommand


class RegisterCommandParser(object):

    @classmethod
    def add_parser(cls, subparsers):
        register_command_parser = subparsers.add_parser(
            "register",
            description="Register a given schema to the schematizer."
        )

        register_command_subparsers = register_command_parser.add_subparsers()
        RegisterAvroCommand.add_parser(register_command_subparsers)
        RegisterMysqlCommand.add_parser(register_command_subparsers)
