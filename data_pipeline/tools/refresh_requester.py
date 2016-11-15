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

from yelp_batch import Batch
from yelp_batch.batch import batch_command_line_options

from data_pipeline import __version__
from data_pipeline.schematizer_clientlib.models.refresh import Priority
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.servlib.config_util import load_package_config


class FullRefreshRequester(Batch):
    """
    FullRefreshRequester parses command line arguments specifying full refresh jobs
    and registers the refresh jobs with the Schematizer.
    """

    def __init__(self):
        super(FullRefreshRequester, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']

    @property
    def version(self):
        """Overriding this so we'll get the clientlib version number when
        the tailer is run with --version.
        """
        return "data_pipeline {}".format(__version__)

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
            '--source-name',
            type=str,
            help="Name of source within --namespace of table to be refreshed "
                 "(--namespace also required)"
        )
        opt_group.add_option(
            '--namespace',
            type=str,
            help="Name of namespace to narrow down sources to be refreshed "
                 "(--source-name also required)"
        )
        opt_group.add_option(
            '--offset',
            dest='offset',
            type='int',
            default=0,
            help='Primary key id to start refreshing from. '
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
        opt_group.add_option(
            '--avg-rows-per-second-cap',
            help='Caps the throughput per second. Important since without any control for this'
            ' the batch can cause signifigant pipeline delays. (default: %default)',
            type='int',
            default=None
        )
        opt_group.add_option(
            '--config-path',
            dest='config_path',
            type='str',
            default='/nail/srv/configs/data_pipeline_tools.yaml',
            help='Config path for Refresh Job (default: %default)'
        )
        return opt_group

    def process_commandline_options(self, args=None):
        super(FullRefreshRequester, self).process_commandline_options(args=args)
        if (self.options.avg_rows_per_second_cap is not None and
                self.options.avg_rows_per_second_cap <= 0):
            raise ValueError("--avg-rows-per-second-cap must be greater than 0")
        if self.options.batch_size <= 0:
            raise ValueError("--batch-size option must be greater than 0.")
        if not self.options.source_id and not (
            self.options.source_name and
            self.options.namespace
        ):
            raise ValueError(
                "--source-id or both of--source-name and --namespace must be defined"
            )
        if self.options.source_id and (
            self.options.source_name or
            self.options.namespace
        ):
            raise ValueError(
                "Cannot use both --source-id and either of --namespace and --source-name"
            )
        load_package_config(self.options.config_path)
        self.schematizer = get_schematizer()
        source_ids = self.get_source_ids()
        if len(source_ids) == 0:
            raise ValueError(
                "Found no sources with namespace_name {} and source_name {}".format(
                    self.options.namespace, self.options.source_name
                )
            )
        elif len(source_ids) > 1:
            raise ValueError(
                "Pair of namespace_name {} and source_name {} somehow received more than one"
                " source. Investigation as to how is recommended.".format(
                    self.options.namespace, self.options.source_name
                )
            )
        self.source_id = source_ids[0]

    def get_source_ids(self):
        if self.options.source_id:
            return [self.options.source_id]
        return [
            source.source_id
            for source in self.schematizer.get_sources_by_namespace(
                self.options.namespace
            ) if source.name == self.options.source_name
        ]

    def create_request(self):
        return self.schematizer.create_refresh(
            source_id=self.source_id,
            offset=self.options.offset,
            batch_size=self.options.batch_size,
            priority=Priority[self.options.priority].value,
            filter_condition=self.options.filter_condition,
            avg_rows_per_second_cap=self.options.avg_rows_per_second_cap
        )

    def run(self):
        request = self.create_request()
        self.log.info(
            "Refresh registered with refresh id: {rid} "
            "on source: {source_name}, namespace: {namespace_name}".format(
                rid=request.refresh_id,
                source_name=request.source_name,
                namespace_name=request.namespace_name
            )
        )


if __name__ == '__main__':
    FullRefreshRequester().start()
