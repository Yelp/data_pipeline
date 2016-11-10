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
