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

from optparse import OptionGroup

from kafka_utils.util.zookeeper import ZK
from kazoo.exceptions import NoNodeError
from yelp_batch import Batch
from yelp_batch.batch import batch_command_line_options

from data_pipeline import __version__
from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.servlib.config_util import load_package_config


class CompactionSetter(Batch):
    """CompactionSetter grabs all kafka topics from the schematizer, filters them by
    whether or not they have a primary_key, and then applies the 'compact' cleanup.policy setting
    on the zookeeper cluster"""

    def __init__(self):
        super(CompactionSetter, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']

    @property
    def version(self):
        """Overriding this so we'll get the clientlib version number when
        the tailer is run with --version.
        """
        return "data_pipeline {}".format(__version__)

    @batch_command_line_options
    def define_options(self, option_parser):
        opt_group = OptionGroup(option_parser, 'Compaction Setter Options')

        opt_group.add_option(
            '--dry-run',
            action="store_true",
            help="If set, will not set any topic configs, but will still run as if it is"
        )
        opt_group.add_option(
            '--config-path',
            dest='config_path',
            type='str',
            default='/nail/srv/configs/data_pipeline_tools.yaml',
            help='Config path for CompactionSetter (default: %default)'
        )
        opt_group.add_option(
            '--whitelist-topic',
            type='str',
            help='A single topic for which log compaction should be turned on'
        )
        return opt_group

    def process_commandline_options(self, args=None):
        super(CompactionSetter, self).process_commandline_options(args=args)

        load_package_config(self.options.config_path)
        self.dry_run = self.options.dry_run
        self.whitelist_topic = self.options.whitelist_topic
        self.schematizer = get_schematizer()

    def _get_all_topics_to_compact(self):
        if self.whitelist_topic:
            topics = [self.schematizer.get_topic_by_name(self.whitelist_topic)]
        else:
            topics = self.schematizer.get_topics_by_criteria()
        topics = [str(topic.name) for topic in topics]
        return self.schematizer.filter_topics_by_pkeys(topics)

    def apply_log_compaction(self, topics):
        self.log.info("Applying compaction settings on {} topics".format(len(topics)))

        compacted_topics = []
        skipped_topics = []
        missed_topics = []

        cluster = get_config().cluster_config

        with ZK(cluster) as zk:
            for topic in topics:
                try:
                    current_config = zk.get_topic_config(topic)
                    if 'cleanup.policy' not in current_config['config']:
                        # if we already have the config set or there was a
                        # manual override we don't want to set again
                        current_config['config']['cleanup.policy'] = 'compact'
                        if not self.dry_run:
                            zk.set_topic_config(topic=topic, value=current_config)
                        compacted_topics.append(topic)
                    else:
                        skipped_topics.append(topic)
                except NoNodeError:
                    missed_topics.append(topic)

        self.log_results(
            compacted_topics=compacted_topics,
            skipped_topics=skipped_topics,
            missed_topics=missed_topics
        )

    def log_results(
        self,
        compacted_topics,
        skipped_topics,
        missed_topics
    ):
        if compacted_topics:
            self.log.info(
                "Applied compaction policy to topics: {compacted_topics}".format(
                    compacted_topics=compacted_topics
                )
            )
        if skipped_topics:
            self.log.info(
                "Skipped topics (already modified cleanup policy): {skipped_topics}".format(
                    skipped_topics=skipped_topics
                )
            )
        if missed_topics:
            self.log.info(
                "Topics not found (most likely from the topics not having any recent messages):"
                "{missed_topics}".format(
                    missed_topics=missed_topics
                )
            )

    def run(self):
        self.log.info("Starting the Datapipeline Compaction Setter")

        topics = self._get_all_topics_to_compact()
        self.log.info("Found {} topics to compact".format(len(topics)))
        self.apply_log_compaction(topics)


if __name__ == '__main__':
    CompactionSetter().start()
