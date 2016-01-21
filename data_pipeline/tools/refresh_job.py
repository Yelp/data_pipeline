# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from optparse import OptionGroup

from yelp_batch import Batch
from yelp_batch.batch import batch_command_line_options
from yelp_servlib.config_util import load_default_config

from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class UndefinedPriorityException(Exception):
    """Raised when user specifies a priority that isn't
    defined by RefreshPriority.
    """
    pass


class FullRefreshJob(Batch):
    """
    FullRefreshJob parses command line arguments specifying full refresh jobs
    and registers the refresh jobs with the Schematizer.
    """

    def __init__(self):
        super(FullRefreshJob, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']
        load_default_config('/nail/srv/configs/data_pipeline_tools.yaml')
        self.schematizer = get_schematizer()

    @batch_command_line_options
    def define_options(self, option_parser):
        opt_group = OptionGroup(option_parser, 'Refresh Job Options')

        opt_group.add_option(
            '--source-id',
            dest='source_id',
            type='int',
            help='Source id of table to be refreshed.'
        )
        opt_group.add_option(
            '--offset',
            dest='offset',
            type='int',
            default=0,
            help='Row offset to start refreshing from. '
                 '(default: %default)'
        )
        opt_group.add_option(
            '--batch-size',
            dest='batch_size',
            type='int',
            default=500,
            help='Number of rows to process between commits '
                 '(default: %default).'
        )
        opt_group.add_option(
            '--priority',
            dest='priority',
            default='MEDIUM',
            help='Priority of this refresh: LOW, MEDIUM, HIGH or MAX '
                 '(default: %default)'
        )
        opt_group.add_option(
            '--filter-condition',
            dest='filter_condition',
            help='Custom WHERE clause to specify which rows to refresh '
                 'Note: This option takes everything that would come '
                 'after the WHERE in a sql statement. '
                 'e.g: --where="country=\'CA\' AND city=\'Waterloo\'"'
        )
        return opt_group

    def validate_priority(self):
        valid_priorities = set(['LOW', 'MEDIUM', 'HIGH', 'MAX'])
        if self.options.priority not in valid_priorities:
            raise UndefinedPriorityException("Priority is not one of: LOW, MEDIUM, HIGH, MAX")

    def run(self):
        self.validate_priority()
        if self.options.batch_size <= 0:
            raise ValueError("--batch-size option must be greater than 0.")
        job = self.schematizer.create_refresh(
            source_id=self.options.source_id,
            offset=self.options.offset,
            batch_size=self.options.batch_size,
            priority=self.options.priority,
            filter_condition=self.options.filter_condition
        )
        self.log.info(
            "Refresh registered with refresh id: {rid} "
            "on source id: {sid}".format(
                rid=job.refresh_id,
                sid=job.source.source_id
            )
        )


if __name__ == '__main__':
    FullRefreshJob().start()
