# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import logging
import simplejson
from collections import OrderedDict

from kafka import KafkaClient
from swaggerpy.exception import HTTPError
from yelp_lib.classutil import cached_property
from yelp_kafka_tool.util.zookeeper import ZK
from yelp_kafka import offsets
from yelp_servlib.config_util import load_package_config

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.schematizer_clientlib.models.topic import Topic

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
        raise NotImplementedError

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

    def _is_topic_in_schematizer(self, topic):
        try:
            self.schematizer.get_topic_by_name(topic)
            return True
        except HTTPError:
            return False

    def _does_topic_range_map_have_messages(self, range_map):
        range_sum = 0
        for _, range in range_map.iteritems():
            range_sum += range
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
            except HTTPError:
                continue
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

    def topic_to_dict(self, topic):
        result_dict = OrderedDict()
        for field in ('name', 'topic_id', 'contains_pii', 'created_at', 'updated_at'):
            value = getattr(topic, field)
            if isinstance(value, datetime.datetime):
                # datetimes are not json serializable
                value = str(value)
        result_dict['source_name'] = topic.source.name
        result_dict['source_id'] = topic.source.source_id
        result_dict['namespace'] = topic.source.namespace.name
        result_dict['in_kafka'] = topic.name in self._kafka_topics
        result_dict['message_count'] = 0
        if topic.name in self.topics_with_messages_to_range_map:
            message_count = 0
            for _, count in self.topics_with_messages_to_range_map[topic.name].iteritems():
                message_count += count
            result_dict['message_count'] = message_count
        return result_dict

    def list_topics(
        self,
        source_id=None,
        source_name=None,
        namespace_name=None
    ):
        if source_id:
            topics = self.schematizer.get_topics_by_source_id(
                    source_id
            )
        else:
            topics = self.schematizer.get_topics_by_criteria(
                namespace_name = namespace_name,
                source_name = source_name
            )
        print simplejson.dumps([self.topic_to_dict(topic) for topic in topics])

    def list_sources(self):
        print "source listing in development"

    def list_namespaces(self):
        print "namespace listing in development"

    def _setup_logging(self):
        CONSOLE_FORMAT = '%(asctime)s - %(name)-12s: %(levelname)-8s %(message)s'

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
        handler.setLevel(logging.INFO)
        self.log.addHandler(handler)

        self.log.setLevel(logging.INFO)
