# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.list_command.namespaces import NamespacesListCommand
from data_pipeline.tools.introspector.list_command.sources import SourcesListCommand
from data_pipeline.tools.introspector.list_command.topics import TopicsListCommand


class ListCommandParser(object):

    @classmethod
    def add_parser(cls, subparsers):
        list_command_parser = subparsers.add_parser(
            "list",
            description="Get a list of specified items as a JSON array of objects."
        )

        list_command_subparsers = list_command_parser.add_subparsers()
        TopicsListCommand.add_parser(list_command_subparsers)
        SourcesListCommand.add_parser(list_command_subparsers)
        NamespacesListCommand.add_parser(list_command_subparsers)
