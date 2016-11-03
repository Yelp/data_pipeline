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

from data_pipeline.tools.introspector.base_command import IntrospectorCommand
from data_pipeline.tools.introspector.models import IntrospectorSource


class SourceInfoCommand(IntrospectorCommand):
    @classmethod
    def add_parser(cls, subparsers):
        info_command_parser = subparsers.add_parser(
            "source",
            description="Get information on a specific data pipeline source.",
            add_help=False
        )

        info_command_parser.add_argument(
            '--active-sources',
            default=False,
            action='store_true',
            help=(
                'If set, this command will also return information about active '
                'topics for this source. '
                'This is a time expensive operation.'
            )
        )

        cls.add_base_arguments(info_command_parser)
        cls.add_source_and_namespace_arguments(info_command_parser)

        info_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_info_source").run(
                args,
                info_command_parser
            )
        )

    def process_args(self, args, parser):
        super(SourceInfoCommand, self).process_args(args, parser)
        self.process_source_and_namespace_args(args, parser)

    def info_source(
        self,
        source_id=None,
        source_name=None,
        namespace_name=None,
        active_sources=False
    ):
        info_source = None
        if source_id:
            info_source = self.schematizer.get_source_by_id(source_id)
        else:
            sources = self.schematizer.get_sources_by_namespace(namespace_name)
            for source in sources:
                if source.name == source_name:
                    info_source = source
                    break
            if not info_source:
                raise ValueError("Given SOURCE_NAME|NAMESPACE_NAME doesn't exist")
        info_source = IntrospectorSource(
            info_source
        ).to_ordered_dict()
        topics = self.list_topics(
            source_id=info_source["source_id"]
        )
        if active_sources:
            info_source['active_topic_count'] = len(
                [topic for topic in topics if topic['message_count']]
            )
        info_source['topics'] = topics
        return info_source

    def run(self, args, parser):
        self.process_args(args, parser)
        print simplejson.dumps(
            self.info_source(
                source_id=self.source_id,
                source_name=self.source_name,
                namespace_name=self.namespace,
                active_sources=args.active_sources
            )
        )
