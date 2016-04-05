# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import logging
from collections import OrderedDict

from kafka import KafkaClient
from swaggerpy.exception import HTTPError
from yelp_kafka import offsets
from yelp_kafka_tool.util.zookeeper import ZK
from yelp_lib.classutil import cached_property
from yelp_servlib.config_util import load_package_config

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class IntrospectorBatch(object):
    """The Data Pipeline Introspector provides information in to the current
    state of the data pipeline using the schematizer, zookeeper, and kafka.

    This is the base class of all of the individual introspection batches.
    """

    def __init__(self, log_name):
        self.log_name = log_name
        load_package_config('/nail/srv/configs/data_pipeline_tools.yaml')
        self._setup_logging()

    @cached_property
    def schematizer(self):
        return get_schematizer()

    @cached_property
    def config(self):
        return get_config()

    @cached_property
    def log(self):
        return logging.getLogger(self.log_name)

    @cached_property
    def kafka_client(self):
        return KafkaClient(self.config.cluster_config.broker_list)

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

    @cached_property
    def _kafka_topics(self):
        with ZK(self.config.cluster_config) as zk:
            return zk.get_topics(
                names_only=True,
                fetch_partition_state=False
            )

    @cached_property
    def all_topic_watermarks(self):
        topics = self._kafka_topics
        return offsets.get_topics_watermarks(
            self.kafka_client,
            topics
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
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise e
        return output

    @cached_property
    def topics_with_messages_to_range_map(self):
        topic_to_range_map = {
            topic: self._topic_watermarks_to_range(topic_watermarks)
            for topic, topic_watermarks in self.all_topic_watermarks.items()
        }
        return {
            topic: range_map
            for topic, range_map in topic_to_range_map.items()
            if (self._does_topic_range_map_have_messages(range_map))
        }

    @cached_property
    def active_topics(self):
        """A cached list of active topics, meaning that they are in both kafka and the schematizer,
        and have at least one message.

        Each topic include all returned values from the schematizer,
        as well as a partition - message_count map (range_map)
        """
        self.log.info("Loading active topics...")
        return self._topic_to_range_map_to_topics_list(self.topics_with_messages_to_range_map)

    @cached_property
    def active_sources(self):
        """A cached dictionary of active sources, meaning that they are a source with an active topic.

        Output is dictionary of source_ids to dict of namespace_name and active_topic_count.
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
        """A cached dictionary of active namespaces, which are namespaces with at least one active source.

        Output is a dictionnary of namespace_name to dict of active_source_count and active_topic_count in the namespace.
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
        if topic.name in self.topics_with_messages_to_range_map:
            message_count = 0
            for _, count in self.topics_with_messages_to_range_map[topic.name].iteritems():
                message_count += count
            return message_count
        return 0

    def schema_to_dict(self, schema):
        result_dict = self._create_serializable_ordered_dict_from_object_and_fields(
            schema,
            ['schema_id', 'base_schema_id', 'status', 'primary_keys', 'created_at',
             'note', 'schema_json']
        )
        return result_dict

    def topic_to_dict(self, topic):
        result_dict = self._create_serializable_ordered_dict_from_object_and_fields(
            topic,
            ['name', 'topic_id', 'primary_keys', 'contains_pii', 'created_at', 'updated_at']
        )
        result_dict['source_name'] = topic.source.name
        result_dict['source_id'] = topic.source.source_id
        result_dict['namespace'] = topic.source.namespace.name
        result_dict['in_kafka'] = topic.name in self._kafka_topics
        result_dict['message_count'] = self._get_topic_message_count(topic)
        return result_dict

    def source_to_dict(self, source, get_active_topic_count=True):
        result_dict = self._create_serializable_ordered_dict_from_object_and_fields(
            source,
            ['name', 'source_id', 'owner_email', 'owner_email']
        )
        if get_active_topic_count:
            active_source = self.active_sources.get(source.source_id, None)
            result_dict['active_topic_count'] = 0 if (
                not active_source
            ) else active_source['active_topic_count']
        result_dict['namespace'] = source.namespace.name
        return result_dict

    def namespace_to_dict(self, namespace):
        result_dict = self._create_serializable_ordered_dict_from_object_and_fields(
            namespace,
            ['name', 'namespace_id']
        )
        active_namespace = self.active_namespaces.get(namespace.name, None)
        if active_namespace:
            result_dict['active_source_count'] = active_namespace['active_source_count']
            result_dict['active_topic_count'] = active_namespace['active_topic_count']
        else:
            result_dict['active_source_count'] = 0
            result_dict['active_topic_count'] = 0
        return result_dict

    def list_schemas(
        self,
        topic_name
    ):
        schemas = self.schematizer.get_schemas_by_topic(topic_name)
        schemas = [self.schema_to_dict(schema) for schema in schemas]
        schemas.sort(key=lambda schema: schema['created_at'], reverse=True)
        return schemas

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
        topics = [self.topic_to_dict(topic) for topic in topics]
        topics.sort(key=lambda topic: topic['updated_at'], reverse=True)
        if sort_by:
            topics.sort(key=lambda topic: topic[sort_by], reverse=descending_order)
        return topics

    def list_sources(
        self,
        namespace_name=None,
        sort_by=None,
        descending_order=False
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
        sources = [self.source_to_dict(source) for source in sources]
        sources.sort(key=lambda source: source['source_id'], reverse=True)
        if sort_by:
            sources.sort(key=lambda source: source[sort_by], reverse=descending_order)
        return sources

    def list_namespaces(
        self,
        sort_by=None,
        descending_order=False
    ):
        namespaces = self.schematizer.get_namespaces()
        namespaces = [self.namespace_to_dict(namespace) for namespace in namespaces]
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

        self.log.setLevel(logging.DEBUG)

    def process_args(self, args, parser):
        self.log.setLevel(max(logging.WARNING - (args.verbosity * 10), logging.DEBUG))
