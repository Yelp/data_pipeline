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


class _BaseRegisterCommand(IntrospectorCommand):

    @classmethod
    def add_base_arguments(cls, parser):
        super(_BaseRegisterCommand, cls).add_base_arguments(parser)
        cls.add_source_and_namespace_arguments(parser)

        parser.add_argument(
            "--source_owner_email",
            type=str,
            required=True,
            help="The email of the owner of the given source."
        )

        parser.add_argument(
            "--contains-pii",
            dest="pii",
            default=False,
            action="store_true",
            help="Flag indicating if schema contains pii. More info at y/pii"
        )

    def process_args(self, args, parser):
        super(_BaseRegisterCommand, self).process_args(args, parser)
        self.process_source_and_namespace_args(args, parser)
        self.source_owner_email = args.source_owner_email
        self.pii = args.pii

    def print_schema(self, schema):
        schema_dict = IntrospectorSchema(
            schema,
            include_topic_info=True
        ).to_ordered_dict()
        print simplejson.dumps(schema_dict)
