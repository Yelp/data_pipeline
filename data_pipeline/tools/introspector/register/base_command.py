# -*- coding: utf-8 -*-
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

        parser.add_argument(
            "--cluster-type",
            dest="cluster_type",
            default='datapipe',
            help="Kafka cluster type to connect. Defaults to datapipe. "
                 "Currectly only datapipe and scribe cluster types are "
                 "supported."
        )

    def process_args(self, args, parser):
        super(_BaseRegisterCommand, self).process_args(args, parser)
        self.process_source_and_namespace_args(args, parser)
        self.source_owner_email = args.source_owner_email
        self.pii = args.pii
        self.cluster_type = args.cluster_type

    def print_schema(self, schema):
        schema_dict = IntrospectorSchema(
            schema,
            include_topic_info=True
        ).to_ordered_dict()
        print simplejson.dumps(schema_dict)
