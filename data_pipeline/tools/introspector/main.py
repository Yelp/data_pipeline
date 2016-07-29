# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

from data_pipeline import __version__
from data_pipeline.tools.introspector.info_command_parser import InfoCommandParser
from data_pipeline.tools.introspector.list_command_parser import ListCommandParser
from data_pipeline.tools.introspector.register_command_parser import RegisterCommandParser
from data_pipeline.tools.introspector.schema_check_command import SchemaCheckCommand


def parse_args():
    parser = argparse.ArgumentParser(
        description="data_pipeline_introspector provides ability to view the current "
        "state of the data pipeline from a top-down view of namespaces."
    )
    parser.add_argument(
        '--version',
        action='version',
        version="data_pipeline {}".format(__version__)
    )

    subparsers = parser.add_subparsers()
    ListCommandParser.add_parser(subparsers)
    InfoCommandParser.add_parser(subparsers)
    RegisterCommandParser.add_parser(subparsers)
    SchemaCheckCommand.add_parser(subparsers)
    return parser.parse_args()


def run():
    args = parse_args()
    args.command(args)


if __name__ == "__main__":
    run()
