# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

from data_pipeline.tools.introspector.list_command import ListCommand
from data_pipeline.tools.introspector.info_command import InfoCommand
from data_pipeline.tools.introspector.register_command import RegisterCommand
from data_pipeline.tools.introspector.schema_check_command import SchemaCheckCommand

def parse_args():
    parser = argparse.ArgumentParser(
        description="data_pipeline_introspector provides ability to view the current "
        "state of the data pipeline from a top-down view of namespaces."
    )

    subparsers = parser.add_subparsers()
    ListCommand.add_parser(subparsers)
    InfoCommand.add_parser(subparsers)
    RegisterCommand.add_parser(subparsers)
    SchemaCheckCommand.add_parser(subparsers)
    return parser.parse_args()

def run():
    args = parse_args()
    args.command(args)

if __name__ == "__main__":
    run()
