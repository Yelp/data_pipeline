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
