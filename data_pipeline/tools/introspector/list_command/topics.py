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


class TopicsListCommand(_BaseListCommand):

    list_type = 'topics'
    fields = [
        'name', 'topic_id', 'contains_pii',
        'primary_keys',
        'in_kafka', 'message_count',
        'source_name', 'source_id',
        'namespace',
        'created_at', 'updated_at'
    ]

    @classmethod
    def add_parser(cls, subparsers):
        list_command_parser = subparsers.add_parser(
            "topics",
            description=cls.get_description(),
            add_help=False
        )

        cls.add_base_arguments(list_command_parser)
        cls.add_source_and_namespace_arguments(list_command_parser)

        list_command_parser.set_defaults(
            command=lambda args:
                cls("data_pipeline_introspector_list").run(args, list_command_parser)
        )

    def process_args(self, args, parser):
        super(TopicsListCommand, self).process_args(args, parser)
        self.process_source_and_namespace_args(args, parser)

    def run(self, args, parser):
        self.process_args(args, parser)
        print simplejson.dumps(self.list_topics(
            source_id=self.source_id,
            namespace_name=self.namespace,
            source_name=self.source_name,
            sort_by=self.sort_by,
            descending_order=self.descending_order
        ))
