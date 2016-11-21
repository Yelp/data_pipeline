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

import re
import signal
import time
from inspect import getmembers
from optparse import OptionGroup
from uuid import UUID

import simplejson
from kafka import KafkaClient
from kafka_utils.util import offsets
from yelp_batch.batch import Batch
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure

from data_pipeline import __version__
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.config import get_config
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import Message
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.servlib.config_util import load_default_config
from data_pipeline.tools.timestamp_to_offset_mapper import get_first_offset_at_or_after_start_timestamp

logger = get_config().logger


class Tailer(Batch):
    """Tailer subscribes to (a) Kafka topic(s) and logs any messages pushed to these,
    applying a transformation on the Avro binary data such that it is readable."""
    enable_error_emails = False

    @property
    def version(self):
        """Overriding this so we'll get the clientlib version number when
        the tailer is run with --version.
        """
        return "data_pipeline {}".format(__version__)

    @batch_command_line_options
    def _define_tailer_options(self, option_parser):
        opt_group = OptionGroup(
            option_parser,
            'Data Pipeline Tailer options'
        )

        opt_group.add_option(
            '--topic',
            type='string',
            action='append',
            dest='topics',
            default=[],
            help=(
                'The topic to tail.  Can be specified multiple times to tail '
                'more than one topic.  Topics can optionally include an offset '
                'by formatting the topic like "topic_name|offset".  If offset '
                'is not specified, its value will come from '
                '--offset-reset-location.'
            ),
        )
        opt_group.add_option(
            '--cluster-name',
            type='string',
            help=(
                'The cluster_name (region) of kafka cluster to connect to. '
                'All topics should belong to the same kafka cluster name. '
                'Defaults to local region where the tailer is running. '
                '(Example: uswest1-devc, uswest2-prod)'
            ),
        )
        opt_group.add_option(
            '--config-file',
            help=(
                'If set, will use the provided configuration file to setup '
                'the data pipeline tools. '
                '(Default is %default)'
            ),
            default='/nail/srv/configs/data_pipeline_tools.yaml'
        )
        opt_group.add_option(
            '--env-config-file',
            help=(
                'If set, will use the provided configuration file as the env '
                'overrides file to setup the data pipeline tools. '
                '(Default is %default)'
            ),
            default=None
        )
        opt_group.add_option(
            '--namespace',
            type='string',
            help=(
                'If given, the namespace and source will be used to lookup '
                'topics, which will be added to any topics otherwise provided. '
                '(Example: refresh_primary.yelp)'
            )
        )
        opt_group.add_option(
            '--source',
            type='string',
            help=(
                'If given, the namespace and source will be used to lookup '
                'topics, which will be added to any topics otherwise provided. '
                '(Example: business)'
            )
        )
        opt_group.add_option(
            '--only-newest',
            action="store_true",
            help=(
                "If given, will limit the topics retrieved from namespace and source to the "
                "topic that has been most recently updated"
            )
        )
        opt_group.add_option(
            '--offset-reset-location',
            type='string',
            default='largest',
            help=(
                'Controls where the Kafka tailer starts from.  Set to smallest '
                'to consume topics from the beginning.  Set to largest to '
                'consume topics from the tail. '
                '(Default is %default)'
            )
        )
        opt_group.add_option(
            '--message-limit',
            type='int',
            default=None,
            help=(
                'Provide a message limit to quit when the specified number of '
                'messages have been output.'
            )
        )
        opt_group.add_option(
            '-f', '--field',
            type='string',
            action='append',
            dest='fields',
            default=['payload_data'],
            help=(
                'The fields of the message to output (default: %default). '
                'The following fields '
                'are available: {}'.format(
                    ', '.join(self._public_message_field_names)
                )
            ),
        )
        opt_group.add_option(
            '--all-fields',
            default=False,
            action='store_true',
            help=(
                'If set, all fields of the message will be output. This is a '
                'shortcut to doing --field for every field name.'
            )
        )
        opt_group.add_option(
            '--json',
            default=False,
            action='store_true',
            help=(
                'If set, the output is converted from into json.'
            )
        )
        opt_group.add_option(
            '--iso-time',
            default=False,
            action='store_true',
            help=(
                'If set, the output for time fields will be in ISO 8601 '
                'format rather than epoch timestamp.'
            )
        )
        opt_group.add_option(
            '--end-timestamp',
            default=None,
            type=int,
            help=(
                'If set, the tailer will only output messages up to the given end date. '
                'Formatted using epoch timestamp.'
            )
        )
        opt_group.add_option(
            '--start-timestamp',
            default=None,
            type=int,
            help=(
                'If set, will set starting offset to be the offset of the first available'
                ' message that comes after or at the given start-timestamp. If a starting offset is'
                ' manually set using the --topic option, then this will not override it.'
                ' Formatted using epoch timestamp'
            )
        )
        return opt_group

    @property
    def _all_message_field_names(self):
        return [
            name for name, value in getmembers(
                Message,
                lambda v: isinstance(v, property)
            )
        ]

    @property
    def _public_message_field_names(self):
        return [
            name for name in self._all_message_field_names
            if not name.startswith('_') and name not in [
                # these fields contain redundant information and as byte-fields
                # can result in UnicodeDecodeError from the simplejson encoder
                'payload',
                'previous_payload',
                'uuid',

                # avro_repr contains redundant information
                'avro_repr',

                # upstream_position_info is for internal use, should always
                # be 'None' to the messages the Tailer will see
                'upstream_position_info',

                # dry_run is always going to be 'False' to messages the Tailer
                # would see
                'dry_run'
            ]
        ]

    @batch_configure
    def _configure_tools(self):
        load_default_config(
            self.options.config_file,
            self.options.env_config_file
        )

        # We setup logging 'early' since we want it available for setup_topics
        self._setup_logging()

        self.kafka_client = KafkaClient(get_config().cluster_config.broker_list)

        self._setup_topics()
        if len(self.topic_to_offsets_map) == 0:
            self.option_parser.error("At least one topic must be specified.")

        if self.options.start_timestamp is not None and self.options.start_timestamp >= int(time.time()):
            self.option_parser.error("--start-timestamp should not be later than current time")

        if self.options.start_timestamp is not None and self.options.end_timestamp and (
            self.options.start_timestamp > self.options.end_timestamp
        ):
            self.option_parser.error("--end-timestamp must not be smaller than --start-timestamp")

        if self.options.all_fields:
            self.options.fields = self._public_message_field_names

        self._verify_offset_ranges()

    def _verify_offset_ranges(self):
        """This is to clarify and enforce only using offsets inside of our actual offset range to avoid
        confusing errors such as those found in DATAPIPE-628"""
        topic_to_partition_offset_map = {
            topic: None if consumer_topic_state is None
            else consumer_topic_state.partition_offset_map
            for topic, consumer_topic_state in self.topic_to_offsets_map.items()
        }
        # If we import get_topics_watermarks directly from offsets, then mock will not properly patch it in testing.
        watermarks = offsets.get_topics_watermarks(
            self.kafka_client,
            topic_to_partition_offset_map,
            # We do not raise on error as we do this verification later on and we
            # want to keep the error message clear
            raise_on_error=False
        )
        for topic, partition_offset_map in topic_to_partition_offset_map.iteritems():
            if partition_offset_map is not None:
                for partition, offset in partition_offset_map.iteritems():
                    highmark = watermarks[topic][partition].highmark
                    lowmark = watermarks[topic][partition].lowmark
                    if offset < lowmark or offset > highmark:
                        self.option_parser.error(
                            "Offset ({}) for topic: {} (partition: {}) is out of range ({}-{})".format(
                                offset,
                                topic,
                                partition,
                                lowmark,
                                highmark
                            )
                        )

    def _setup_topics(self):
        self.topic_to_offsets_map = {}
        self._setup_manual_topics()
        self._setup_schematizer_topics()
        if self.options.start_timestamp is not None:
            self._setup_start_timestamp_topics(self.options.start_timestamp)

    def _setup_manual_topics(self):
        for topic in self.options.topics:
            offset = None

            # https://regex101.com/r/fL0eD9/3
            match = re.match('^(.*)\|(\d+)$', topic)
            if match:
                topic = match.group(1)
                offset = ConsumerTopicState({0: int(match.group(2))}, None)

            self.topic_to_offsets_map[str(topic)] = offset

    def _setup_schematizer_topics(self):
        if self.options.namespace or self.options.source:
            schematizer = get_schematizer()
            additional_topics = schematizer.get_topics_by_criteria(
                namespace_name=self.options.namespace,
                source_name=self.options.source
            )
            if self.options.only_newest:
                additional_topics = self._filter_by_most_recently_updated(additional_topics)
            logger.info(
                "Received {} new topics from --source and --namespace options".format(
                    len(additional_topics)
                )
            )
            for topic in additional_topics:
                if str(topic.name) not in self.topic_to_offsets_map:
                    self.topic_to_offsets_map[str(topic.name)] = None

    def _filter_by_most_recently_updated(self, topics):
        if not topics:
            return []
        sorted_topics = sorted(topics, key=lambda topic: topic.updated_at)
        return [sorted_topics[-1]]

    def _setup_start_timestamp_topics(self, start_timestamp):
        """Sets the offsets of all topics with no offset to be that topic's first offset
        that comes after --start-timestamp"""
        no_offset_topics = [
            topic
            for topic, offset in self.topic_to_offsets_map.iteritems()
            if offset is None
        ]
        logger.info(
            "Getting starting offsets for {} based on --start-timestamp".format(no_offset_topics)
        )
        start_timestamp_topic_to_offset_map = get_first_offset_at_or_after_start_timestamp(
            self.kafka_client,
            no_offset_topics,
            start_timestamp
        )
        for topic, consumer_topic_state in start_timestamp_topic_to_offset_map.iteritems():
            self.topic_to_offsets_map[topic] = start_timestamp_topic_to_offset_map[topic]

    @batch_configure
    def _configure_signals(self):
        self._running = True

        def handle_signal(signum, frame):
            self._running = False
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

    def run(self):
        logger.info(
            "Starting to consume from {}".format(self.topic_to_offsets_map)
        )

        with Consumer(
            # The tailer name should be unique - if it's not, partitions will
            # be split between multiple tailer instances
            'data_pipeline_tailer-{}'.format(
                str(UUID(bytes=FastUUID().uuid4()).hex)
            ),
            'bam',
            ExpectedFrequency.constantly,
            self.topic_to_offsets_map,
            auto_offset_reset=self.options.offset_reset_location,
            cluster_name=self.options.cluster_name
        ) as consumer:
            message_count = 0
            while self.keep_running(message_count):
                message = consumer.get_message(blocking=True, timeout=0.1)
                if message is not None:
                    if self.options.end_timestamp is None or message.timestamp < self.options.end_timestamp:
                        print self._format_message(message)
                        message_count += 1
                    else:
                        self._running = False
                        logger.info(
                            "Latest message surpasses --end-timestamp. Stopping tailer..."
                        )

    def _get_message_result_dict(self, message):
        return {
            field: getattr(message, field)
            for field in self.options.fields if hasattr(message, field)
        }

    def _walk_dict(self, node, transform_item):
        for key, item in node.items():
            if isinstance(item, dict):
                self._walk_dict(item, transform_item)
            else:
                node[key] = transform_item(key, item)

    def _iso_time(self, result_dict):
        def _transform_item_iso_time(key, item):
            if key.startswith('time') and isinstance(item, int):
                return time.strftime(
                    '%Y-%m-%dT%H:%M:%S',
                    time.gmtime(item)
                )
            return item
        self._walk_dict(result_dict, _transform_item_iso_time)

    def _format_message(self, message):
        result_dict = self._get_message_result_dict(message)
        if self.options.iso_time:
            self._iso_time(result_dict)
        if self.options.json:
            return simplejson.dumps(
                obj=result_dict,
                sort_keys=True,

                # Objects can use _asdict() to be encoded as JSON objects
                namedtuple_as_object=True,

                # Use an object's __repr__() to return a serializable version
                # of an object, rather than raising a TypeError, if the object
                # does not define an _asdict() method
                default=lambda x: repr(x)
            )
        else:
            return result_dict

    def keep_running(self, message_count):
        return self._running and (
            self.options.message_limit is None or
            message_count < self.options.message_limit
        )


if __name__ == '__main__':
    Tailer().start()
