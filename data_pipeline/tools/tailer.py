# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import signal
from optparse import OptionGroup
from uuid import UUID

from yelp_batch.batch import Batch
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure
from yelp_servlib.config_util import load_default_config

from data_pipeline._fast_uuid import FastUUID
from data_pipeline.config import get_config
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class Tailer(Batch):
    enable_error_emails = False

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
                'more than one topic.'
            ),
        )
        opt_group.add_option(
            '--config-file',
            action='store_true',
            help=(
                'If set, will use the provided configuration file to setup '
                'the data pipeline tools. '
                '(Default is %default)'
            ),
            default='/nail/srv/configs/data_pipeline_tools.yaml'
        )
        opt_group.add_option(
            '--env-config-file',
            action='store_true',
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

        return opt_group

    @batch_configure
    def _configure_tools(self):
        load_default_config(
            self.options.config_file,
            self.options.env_config_file
        )

        if self.options.namespace or self.options.source:
            schematizer = get_schematizer()
            additional_topics = schematizer.get_topics_by_criteria(
                namespace_name=self.options.namespace,
                source_name=self.options.source
            )
            self.options.topics.extend(
                [topic.name for topic in additional_topics]
            )

        if len(self.options.topics) == 0:
            self.option_parser.error("At least one topic must be specified.")

    @batch_configure
    def _configure_signals(self):
        self._running = True

        def handle_signal(signum, frame):
            self._running = False
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

    def run(self):
        get_config().logger.info(
            "Starting to consume from {}".format(self.options.topics)
        )

        with Consumer(
            # The tailer name should be unique - if it's not, partitions will
            # be split between multiple tailer instances
            'data_pipeline_tailer-{}'.format(
                str(UUID(bytes=FastUUID().uuid4()).hex)
            ),
            'bam',
            ExpectedFrequency.constantly,
            {str(topic): None for topic in self.options.topics},
            auto_offset_reset=self.options.offset_reset_location
        ) as consumer:
            while self.running:
                message = consumer.get_message(blocking=True, timeout=0.1)
                if message is not None:
                    # TODO(justinc|DATAPIPE-522): We should allow additional
                    # output options.
                    print message.payload_data

    @property
    def running(self):
        return self._running


if __name__ == '__main__':
    Tailer().start()
