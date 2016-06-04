# -*- coding: utf-8 -*-
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
