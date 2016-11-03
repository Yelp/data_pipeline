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

from data_pipeline.tools.introspector.base_command import IntrospectorCommand


class _BaseListCommand(IntrospectorCommand):

    @classmethod
    def add_base_arguments(cls, parser):
        super(_BaseListCommand, cls).add_base_arguments(parser)

        parser.add_argument(
            "-s",
            "--sort-by",
            type=str,
            default=None,
            help="Sort the listing by a particular field of the object "
                 "in ascending order (by default)"
        )

        parser.add_argument(
            "--descending-order", "--desc",
            action="store_true",
            default=False,
            help="Use --sort-by with descending order (Will be ignored if --sort-by is not set)"
        )

    @classmethod
    def get_description(cls):
        return "List {}, as a JSON array of formatted {}. Fields: {}".format(
            cls.list_type, cls.list_type, cls.fields
        )

    def process_args(self, args, parser):
        super(_BaseListCommand, self).process_args(args, parser)
        self.sort_by = args.sort_by
        self.descending_order = args.descending_order
        if self.sort_by and self.sort_by not in self.fields:
            raise parser.error(
                "You can not sort_by by {} for list type {}. Possible fields are: {}".format(
                    self.sort_by, self.list_type, self.fields
                )
            )
