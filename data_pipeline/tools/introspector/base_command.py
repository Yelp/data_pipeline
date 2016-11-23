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

import datetime
import logging
from collections import OrderedDict
from contextlib import contextmanager

from bravado.exception import HTTPError
from cached_property import cached_property
from kafka import KafkaClient
from kafka_utils.util import offsets
from kafka_utils.util.zookeeper import ZK

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.servlib.config_util import load_package_config
from data_pipeline.tools.introspector.models import IntrospectorNamespace
from data_pipeline.tools.introspector.models import IntrospectorSource
from data_pipeline.tools.introspector.models import IntrospectorTopic


class IntrospectorCommand(object):
    """The Data Pipeline Introspector provides information in to the current
    state of the data pipeline using the schematizer, zookeeper, and kafka.

    This is the base class of all of the individual introspection batches.
    """

    def __init__(self, log_name):
        self.log_name = log_name
        load_package_config('/nail/srv/configs/data_pipeline_tools.yaml')
        self.config = get_config()
        self.log = logging.getLogger(self.log_name)
        self._setup_logging()
        self.schematizer = get_schematizer()

    @classmethod
    def add_parser(cls, subparsers):
        raise NotImplementedError("All InspectorBatch sub classes must have add_parser defined")

    @classmethod
    def add_base_arguments(cls, parser):
        parser.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show help messages and exit."
        )

        parser.add_argument(
            "-v", "--verbose",
            dest="verbosity",
            default=0,
            action="count",
            help="Adjust log level"
        )

    @classmethod
    def add_source_and_namespace_arguments(cls, parser):
        parser.add_argument(
            "--source-id",
            type=int,
            required=False,
            default=None,
            help="Source id of source to check against"
        )

        parser.add_argument(
            "--source-name",
            type=str,
            required=False,
            default=None,
            help="Source name of source to check against. "
                 "Must be used with --namespace"
        )

        parser.add_argument(
            "--namespace",
            required=False,
            type=str,
            default=None,
            help="Namespace name that contains a source of source name given. "
                 "If --source-id is given, then this will be ignored."
        )

    @cached_property
    def _kafka_topics(self):
        with ZK(self.config.cluster_config) as zk:
            return zk.get_topics(
                names_only=True,
                fetch_partition_state=False
            )

    @cached_property
    def _all_topic_watermarks(self):
        topics = self._kafka_topics
        with self._kafka_client() as kafka_client:
            return offsets.get_topics_watermarks(
                kafka_client,
                topics
            )

    @contextmanager
    def _kafka_client(self):
        kafka_client = KafkaClient(self.config.cluster_config.broker_list)
        try:
            yield kafka_client
        finally:
            kafka_client.close()

    def _retrieve_names_from_source_id(self, source_id):
        """Returns (source_name, namespace_name) of source with given source_id"""
        source = self.schematizer.get_source_by_id(source_id)
        return (source.name, source.namespace.name)

    def process_source_and_namespace_args(self, args, parser):
        self.source_id = args.source_id
        self.source_name = args.source_name
        self.namespace = args.namespace
        if self.source_id:
            self.source_name, self.namespace = self._retrieve_names_from_source_id(
                self.source_id
            )
            if args.namespace:
                self.log.warning(
                    "Since source id was given, --namespace will be ignored"
                )
        else:
            if not self.namespace:
                raise parser.error(
                    "--namespace must be provided when given a source name as source identifier."
                )

    def _does_topic_range_map_have_messages(self, range_map):
        range_sum = 0
        for _, watermark_range in range_map.iteritems():
            range_sum += watermark_range
        return range_sum > 0

    def _topic_watermarks_to_range(self, watermarks):
        return {
            partition: partition_offsets.highmark - partition_offsets.lowmark
            for partition, partition_offsets in watermarks.items()
        }

    def _topic_to_range_map_to_topics_list(self, topic_to_range_map):
        output = []
        for topic, range_map in topic_to_range_map.iteritems():
            try:
                topic_result = self.schematizer.get_topic_by_name(topic)._asdict()
                topic_result['range_map'] = range_map
                output.append(topic_result)
            except HTTPError as error:
                if error.response.status_code != 404:
                    raise
        return output

    @cached_property
    def _topics_with_messages_to_range_map(self):
        topic_to_range_map = {
            topic: self._topic_watermarks_to_range(topic_watermarks)
            for topic, topic_watermarks in self._all_topic_watermarks.items()
        }
        return {
            topic: range_map
            for topic, range_map in topic_to_range_map.items()
            if (self._does_topic_range_map_have_messages(range_map))
        }

    @cached_property
    def active_topics(self):
        """A cached list of active topics,
        meaning that they are in both kafka and the schematizer,
        and have at least one message.

        Each topic include all returned values from the schematizer,
        as well as a partition - message_count map (range_map)
        """
        self.log.info("Loading active topics...")
        return self._topic_to_range_map_to_topics_list(self._topics_with_messages_to_range_map)

    @cached_property
    def active_sources(self):
        """A cached dictionary of active sources, meaning that they are
        a source with an active topic.

        Output is dictionary of source_ids to
        dict of namespace_name and active_topic_count.
        """
        self.log.info("Loading active sources...")
        active_sources = {}
        for topic in self.active_topics:
            source_id = topic['source'].source_id
            if source_id in active_sources:
                active_sources[source_id]['active_topic_count'] += 1
            else:
                active_sources[source_id] = {
                    'namespace_name': topic['source'].namespace.name,
                    'active_topic_count': 1
                }
        return active_sources

    @cached_property
    def active_namespaces(self):
        """A cached dictionary of active namespaces, which are namespaces
        with at least one active source.

        Output is a dictionnary of namespace_name to dict of active_source_count
        and active_topic_count in the namespace.
        """
        self.log.info("Loading active namespaces...")
        active_namespaces = {}
        for source_id, active_source in self.active_sources.iteritems():
            namespace_name = active_source['namespace_name']
            active_topic_count = active_source['active_topic_count']
            if namespace_name in active_namespaces:
                active_namespaces[namespace_name]['active_source_count'] += 1
                active_namespaces[namespace_name]['active_topic_count'] += active_topic_count
            else:
                active_namespaces[namespace_name] = {
                    'active_source_count': 1,
                    'active_topic_count': active_topic_count
                }
        return active_namespaces

    def _create_serializable_ordered_dict_from_object_and_fields(self, obj, fields):
        result_dict = OrderedDict()
        for field in fields:
            value = getattr(obj, field)
            if isinstance(value, datetime.datetime):
                # datetimes are not json serializable
                value = str(value)
            result_dict[field] = value
        return result_dict

    def _get_topic_message_count(self, topic):
        if topic.name in self._topics_with_messages_to_range_map:
            message_count = 0
            for _, count in self._topics_with_messages_to_range_map[topic.name].iteritems():
                message_count += count
            return message_count
        return 0

    def list_topics(
        self,
        source_id=None,
        source_name=None,
        namespace_name=None,
        sort_by=None,
        descending_order=False
    ):
        if source_id:
            topics = self.schematizer.get_topics_by_source_id(
                source_id
            )
        else:
            topics = self.schematizer.get_topics_by_criteria(
                namespace_name=namespace_name,
                source_name=source_name
            )
        topics = [
            IntrospectorTopic(
                topic,
                kafka_topics=self._kafka_topics,
                topics_to_range_map=self._topics_with_messages_to_range_map
            ).to_ordered_dict() for topic in topics
        ]
        topics.sort(key=lambda topic: topic['updated_at'], reverse=True)
        if sort_by:
            topics.sort(key=lambda topic: topic[sort_by], reverse=descending_order)
        return topics

    def list_sources(
        self,
        namespace_name=None,
        sort_by=None,
        descending_order=False,
        active_sources=False
    ):
        if namespace_name:
            sources = self.schematizer.get_sources_by_namespace(namespace_name)
        else:
            self.log.info("Getting all namespaces...")
            namespaces = self.schematizer.get_namespaces()
            sources = []
            self.log.info("Getting all sources for each namespace...")
            for namespace in namespaces:
                sources += self.schematizer.get_sources_by_namespace(namespace.name)
        sources = [
            IntrospectorSource(
                source,
                active_sources=(self.active_sources if active_sources else None)
            ).to_ordered_dict() for source in sources
        ]
        sources.sort(key=lambda source: source['source_id'], reverse=True)
        if sort_by:
            sources.sort(key=lambda source: source[sort_by], reverse=descending_order)
        return sources

    def list_namespaces(
        self,
        sort_by=None,
        descending_order=False,
        active_namespaces=False
    ):
        namespaces = self.schematizer.get_namespaces()
        namespaces = [
            IntrospectorNamespace(
                namespace,
                active_namespaces=(self.active_namespaces if active_namespaces else None)
            ).to_ordered_dict() for namespace in namespaces
        ]
        namespaces.sort(key=lambda namespace: namespace['namespace_id'], reverse=True)
        if sort_by:
            namespaces.sort(key=lambda namespace: namespace[sort_by], reverse=descending_order)
        return namespaces

    def _setup_logging(self):
        CONSOLE_FORMAT = '%(asctime)s - %(name)-12s: %(levelname)-8s %(message)s'

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
        handler.setLevel(logging.DEBUG)
        self.log.addHandler(handler)

    def process_args(self, args, parser):
        self.log.setLevel(max(logging.WARNING - (args.verbosity * 10), logging.DEBUG))
