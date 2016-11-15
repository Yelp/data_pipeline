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

import simplejson

from data_pipeline.tools.introspector.list_command.base_command import _BaseListCommand


class SourcesListCommand(_BaseListCommand):

    list_type = 'sources'
    fields = [
        'name', 'source_id', 'owner_email',
        'namespace', 'active_topic_count'
    ]

    @classmethod
    def add_parser(cls, subparsers):
        list_command_parser = subparsers.add_parser(
            "sources",
            description=cls.get_description(),
            add_help=False
        )

        list_command_parser.add_argument(
            '--active-sources',
            default=False,
            action='store_true',
            help=(
                'If set, this command will also return information about active '
                'topics within each source. This is a time expensive operation.'
            )
        )

        cls.add_base_arguments(list_command_parser)

        list_command_parser.add_argument(
            "--namespace",
            type=str,
            default=None,
            help="Namespace name that contains a source of source name given. "
                 "If --source-id is given, then this will be ignored."
        )

        list_command_parser.set_defaults(
            command=lambda args:
                cls("data_pipeline_introspector_list").run(args, list_command_parser)
        )

    def process_args(self, args, parser):
        super(SourcesListCommand, self).process_args(args, parser)
        self.namespace = args.namespace

    def run(self, args, parser):
        self.process_args(args, parser)
        print simplejson.dumps(self.list_sources(
            namespace_name=self.namespace,
            sort_by=self.sort_by,
            descending_order=self.descending_order,
            active_sources=args.active_sources
        ))
