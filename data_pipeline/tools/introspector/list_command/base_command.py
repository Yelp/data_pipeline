# -*- coding: utf-8 -*-
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
