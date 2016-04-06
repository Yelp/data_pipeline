# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import pprint

import simplejson

from data_pipeline.tools.introspector.base import IntrospectorBatch


class ListCommand(IntrospectorBatch):

    list_type_to_fields_map = {
        'topics': [
            'name', 'topic_id', 'contains_pii',
            'primary_keys',
            'in_kafka', 'message_count',
            'source_name', 'source_id',
            'namespace',
            'created_at', 'updated_at'
        ],
        'sources': [
            'name', 'source_id', 'owner_email',
            'namespace', 'active_topic_count'
        ],
        'namespaces': [
            'name', 'namespace_id',
            'active_topic_count', 'active_source_count'
        ]
    }

    @classmethod
    def add_parser(cls, subparsers):
        list_command_parser = subparsers.add_parser(
            "list",
            description="List the specified items, with a row for each item.\n"
                        "The fields of each type are as follows: \n{}\n\n"
                        "NOTE: It is required to use either --source-filter with a source id or \n"
                        "both of --namespace-filter and --source-filter (as source name) to list topics\n".format(
                            pprint.pformat(cls.list_type_to_fields_map)
                        ),
            # Use the raw formatter so that the fields can be printed in a useable way
            formatter_class=argparse.RawDescriptionHelpFormatter,
            add_help=False
        )

        cls.add_base_arguments(list_command_parser)

        list_command_parser.add_argument(
            "list_type",
            type=str,
            choices=[list_type for list_type in cls.list_type_to_fields_map.keys()],
            help="What type of object you want to list. "
        )

        list_command_parser.add_argument(
            "-s",
            "--sort-by",
            type=str,
            default=None,
            help="Sort the listing by a particular field of the object in ascending order (by default)"
        )

        list_command_parser.add_argument(
            "--descending-order", "--desc",
            action="store_true",
            default=False,
            help="Use --sort-by with descending order (Will be ignored if --sort-by is not set)"
        )

        list_command_parser.add_argument(
            "--namespace-filter",
            type=str,
            default=None,
            help="Filter by namespace, using the namespace's name. "
                 "Note: Cannot filter list namespaces by a particular namespace (will be ignored). "
                 "Will be overrided if filtering by source id"
        )

        list_command_parser.add_argument(
            "--source-filter",
            type=str,
            default=None,
            help="Filter by source. If it is an integer, it will use this as an id. "
                 "Otherwise, we will treat it as a source name to be used in tandem with --namespace-filter. "
                 "Note: Can only filter topics by sources (will otherwise be ignored). "
                 "Will override all other filters"
        )

        list_command_parser.set_defaults(
            command=lambda args:
                cls("data_pipeline_introspector_list").run(args, list_command_parser)
        )

    def process_args(self, args, parser):
        super(ListCommand, self).process_args(args, parser)
        self.list_type = args.list_type
        self.fields = self.list_type_to_fields_map[self.list_type]
        self.sort_by = args.sort_by
        self.descending_order = args.descending_order
        if self.sort_by and self.sort_by not in self.fields:
            raise parser.error(
                "You can not sort_by by {} for list type {}. Possible fields are: {}".format(
                    self.sort_by, self.list_type, self.fields
                )
            )

        self.namespace_filter = args.namespace_filter
        self.source_id_filter = None
        self.source_name_filter = None
        if args.source_filter and args.source_filter.isdigit():
            self.source_id_filter = int(args.source_filter)
        else:
            self.source_name_filter = args.source_filter

        if self.list_type == "topics" and not (
            (self.namespace_filter and self.source_name_filter) or self.source_id_filter
        ):
            raise parser.error(
                "Must provide topic filters of --source-filter with a source id or both of " +
                "--namespace-filter and --source-filter with a source name"
            )
        if self.list_type == "namespaces" and self.namespace_filter:
            self.log.warning("Will not use --namespace-filter to filter namespaces")
        if (self.list_type == "sources" or self.list_type == "namespaces") and (
            self.source_name_filter or self.source_id_filter
        ):
            self.log.warning(
                "Will not use --source-filter to filter {}".format(
                    self.list_type
                )
            )
        if (
            self.list_type == "topics" and
            self.source_id_filter and (
                self.source_name_filter or self.namespace_filter
            )
        ):
            self.log.warning(
                "Overriding all other filters with --source-filter"
            )

    def run(self, args, parser):
        self.process_args(args, parser)
        if self.list_type == "topics":
            print simplejson.dumps(self.list_topics(
                source_id=self.source_id_filter,
                namespace_name=self.namespace_filter,
                source_name=self.source_name_filter,
                sort_by=self.sort_by,
                descending_order=self.descending_order
            ))
        elif self.list_type == "sources":
            print simplejson.dumps(self.list_sources(
                namespace_name=self.namespace_filter,
                sort_by=self.sort_by,
                descending_order=self.descending_order
            ))
        else:
            print simplejson.dumps(self.list_namespaces(
                sort_by=self.sort_by,
                descending_order=self.descending_order
            ))
