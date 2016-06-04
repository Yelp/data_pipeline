# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.tools.introspector.info.namespace import NamespaceInfoCommand
from data_pipeline.tools.introspector.info.source import SourceInfoCommand
from data_pipeline.tools.introspector.info.topic import TopicInfoCommand


class InfoCommandParser(object):

    @classmethod
    def add_parser(cls, subparsers):
        info_command_parser = subparsers.add_parser(
            "info",
            description="Get information on a specific data pipeline item."
        )

        info_command_subparsers = info_command_parser.add_subparsers()
        TopicInfoCommand.add_parser(info_command_subparsers)
        SourceInfoCommand.add_parser(info_command_subparsers)
        NamespaceInfoCommand.add_parser(info_command_subparsers)
