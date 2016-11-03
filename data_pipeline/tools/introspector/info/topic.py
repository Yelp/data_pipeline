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
from data_pipeline.tools.introspector.models import IntrospectorSchema
from data_pipeline.tools.introspector.models import IntrospectorTopic


class TopicInfoCommand(IntrospectorCommand):
    @classmethod
    def add_parser(cls, subparsers):
        info_command_parser = subparsers.add_parser(
            "topic",
            description="Get information on a specific data pipeline topic.",
            add_help=False
        )

        cls.add_base_arguments(info_command_parser)

        info_command_parser.add_argument(
            "topic_name",
            type=str,
            help="Name of topic to retrieve information on."
        )

        info_command_parser.set_defaults(
            command=lambda args: cls("data_pipeline_instropsector_info_topic").run(
                args,
                info_command_parser
            )
        )

    def info_topic(self, name):
        topic = self.schematizer.get_topic_by_name(name)
        topic = IntrospectorTopic(
            topic,
            kafka_topics=self._kafka_topics,
            topics_to_range_map=self._topics_with_messages_to_range_map
        ).to_ordered_dict()
        topic['schemas'] = self.list_schemas(name)
        return topic

    def list_schemas(
        self,
        topic_name
    ):
        schemas = self.schematizer.get_schemas_by_topic(topic_name)
        schemas = [IntrospectorSchema(schema).to_ordered_dict() for schema in schemas]
        schemas.sort(key=lambda schema: schema['created_at'], reverse=True)
        return schemas

    def process_args(self, args, parser):
        super(TopicInfoCommand, self).process_args(args, parser)
        self.topic_name = args.topic_name

    def run(self, args, parser):
        self.process_args(args, parser)
        print simplejson.dumps(
            self.info_topic(self.topic_name)
        )
