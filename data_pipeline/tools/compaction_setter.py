# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from optparse import OptionGroup

from kazoo.exceptions import NoNodeError
from yelp_batch import Batch
from yelp_batch.batch import batch_command_line_options
from yelp_kafka_tool.util.zookeeper import ZK
from yelp_servlib.config_util import load_package_config

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class CompactionSetter(Batch):
    """CompactionSetter grabs all kafka topics from the schematizer, filters them by
    whether or not they have a primary_key, and then applies the 'compact' cleanup.policy setting
    on the zookeeper cluster"""

    def __init__(self):
        super(CompactionSetter, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']

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
        return opt_group

    def process_commandline_options(self, args=None):
        super(CompactionSetter, self).process_commandline_options(args=args)

        load_package_config(self.options.config_path)
        self.dry_run = self.options.dry_run
        self.schematizer = get_schematizer()

    def get_all_topics(self):
        namespaces = self.schematizer.get_namespaces()
        topics = []
        for namespace in namespaces:
            topics += self.schematizer.get_topics_by_criteria(
                namespace_name=namespace.name
            )
        return topics

    def get_all_topics_to_compact(self):
        topics = self.get_all_topics()
        topics = [topic.name for topic in topics]
        self.log.debug("Found {} topics to filter".format(len(topics)))
        return self.schematizer.filter_topics_by_pkeys(topics)

    def apply_log_compaction(self, topics):
        self.log.info("Applying compaction settings on {} topics".format(len(topics)))

        self.compacted_topics = []
        self.skipped_topics = []
        self.missed_topics = []

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
                        self.compacted_topics.append(topic)
                    else:
                        self.skipped_topics.append(topic)
                except NoNodeError:
                    self.missed_topics.append(topic)

        self.log_results()

    def log_results(self):
        if self.compacted_topics:
            self.log.info(
                "Applied compaction policy to topics: {compacted_topics}".format(
                    compacted_topics=self.compacted_topics
                )
            )
        if self.skipped_topics:
            self.log.info(
                "Skipped topics (already modified cleanup policy): {skipped_topics}".format(
                    skipped_topics=self.skipped_topics
                )
            )
        if self.missed_topics:
            self.log.info(
                "Topics not found (most likely from the topics not having any recent messages):"
                "{missed_topics}".format(
                    missed_topics=self.missed_topics
                )
            )

    def run(self):
        self.log.info("Starting the Datapipeline Compaction Setter")

        topics = self.get_all_topics_to_compact()
        self.log.info("Found {} topics to compact".format(len(topics)))
        self.apply_log_compaction(topics)

if __name__ == '__main__':
    CompactionSetter().start()
