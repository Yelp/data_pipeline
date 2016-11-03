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

import os
import signal
import sys
import time
from multiprocessing import Process
from optparse import OptionGroup

import psutil
from cached_property import cached_property
from yelp_batch import BatchDaemon
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure

from data_pipeline import __version__
from data_pipeline._namespace_util import DBSourcedNamespace
from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.servlib.config_util import load_package_config
from data_pipeline.tools.copy_table_to_blackhole_table import FullRefreshRunner
from data_pipeline.zookeeper import ZKLock

SCHEMATIZER_POLL_FREQUENCY_SECONDS = 5
COMPLETE_STATUSES = {RefreshStatus.SUCCESS, RefreshStatus.FAILED}


class FullRefreshManager(BatchDaemon):

    def __init__(self):
        super(FullRefreshManager, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']
        self.active_refresh = {'id': None, 'pid': None}

    @cached_property
    def schematizer(self):
        return get_schematizer()

    @property
    def version(self):
        """Overriding this so we'll get the clientlib version number when
        the tailer is run with --version.
        """
        return "data_pipeline {}".format(__version__)

    @batch_command_line_options
    def define_options(self, option_parser):
        opt_group = OptionGroup(option_parser, 'Full Refresh Manager Options')

        opt_group.add_option(
            '--namespace',
            type=str,
            default=None,
            help='Name of the namespace this refresh manager will handle. Expected format: '
            '"cluster.database"'
        )
        opt_group.add_option(
            '--config-path',
            dest='config_path',
            default='/nail/srv/configs/data_pipeline_tools.yaml',
            help='Config file path for the refresh manager. '
                 '(default: %default)'
        )
        opt_group.add_option(
            '--dry-run',
            action="store_true",
            default=False,
            dest="dry_run",
            help="Will execute all refreshes as dry runs, will still affect "
            "the schematizer's records. (default: %default)"
        )
        return opt_group

    @batch_configure
    def _init_global_state(self):
        if self.options.namespace is None:
            raise ValueError("--namespace is required to be defined")
        self.namespace = self.options.namespace
        self._set_cluster_and_database()
        self.config_path = self.options.config_path
        self.dry_run = self.options.dry_run
        load_package_config(self.config_path)
        # Removing the cmd line arguments to prevent child process error.
        sys.argv = sys.argv[:1]

    def _set_cluster_and_database(self):
        namespace_info = DBSourcedNamespace.create_from_namespace_name(self.namespace)
        self.cluster = namespace_info.cluster
        self.database = namespace_info.database

    def _begin_refresh_job(self, refresh):
        # We need to make 2 schematizer requests to get the primary_keys, but this happens infrequently enough
        # where it's not paricularly vital to make a new end point for it
        topic = self.schematizer.get_latest_topic_by_source_id(refresh.source.source_id)
        primary_keys = self.schematizer.get_latest_schema_by_topic_name(topic.name).primary_keys
        if len(primary_keys):
            primary_keys = ','.join(primary_keys)
        else:
            # Fallback in case we get bad data from the schematizer (command line default is 'id')
            primary_keys = 'id'
        refresh_batch = FullRefreshRunner(
            refresh_id=self.active_refresh['id'],
            cluster=self.cluster,
            database=self.database,
            config_path=self.config_path,
            table_name=refresh.source.name,
            offset=refresh.offset,
            batch_size=refresh.batch_size,
            primary=primary_keys,
            where_clause=refresh.filter_condition,
            dry_run=self.dry_run,
            avg_rows_per_second_cap=getattr(refresh, 'avg_rows_per_second_cap', None)
        )
        self.log.info(
            "Starting a batch table_name: {}, refresh_id: {}, worker_id: {}".format(
                refresh.source.name,
                self.active_refresh['id'],
                self.active_refresh['pid']
            )
        )
        refresh_batch.start()

    def setup_new_refresh(self, refresh):
        active_pid = self.active_refresh['pid']
        if active_pid is not None:
            # If we somehow have an active worker but not an active refresh (refresh died somehow),
            # then we want to kill that worker.
            os.kill(active_pid, signal.SIGTERM)
        new_worker = Process(
            target=self._begin_refresh_job,
            args=(refresh,)
        )
        self.active_refresh['id'] = refresh.refresh_id
        new_worker.start()
        self.active_refresh['pid'] = new_worker.pid

    def _should_run_next_refresh(self, next_refresh):
        if not self.active_refresh['id']:
            return True
        current_refresh = self.schematizer.get_refresh_by_id(
            self.active_refresh['id']
        )
        next_priority = next_refresh.priority.value
        current_priority = current_refresh.priority.value
        return (next_priority > current_priority or
                current_refresh.status in COMPLETE_STATUSES)

    def determine_best_refresh(self, not_started_jobs, paused_jobs):
        if not_started_jobs and paused_jobs:
            not_started_job = not_started_jobs[0]
            paused_job = paused_jobs[0]
            if not_started_job.priority.value > paused_job.priority.value:
                return not_started_job
            else:
                return paused_job

        if not_started_jobs:
            return not_started_jobs[0]

        if paused_jobs:
            return paused_jobs[0]

        return None

    def set_zombie_refresh_to_fail(self):
        """The manager sometimes gets in to situations where
        the worker becomes a zombie but the refresh stays 'IN_PROGRESS'.
        For these situations we want to correct the refresh status to failed.
        """
        current_pid = self.active_refresh['pid']
        if current_pid is None:
            return

        p = psutil.Process(current_pid)
        if p.status() != psutil.STATUS_ZOMBIE:
            return

        refresh = self.schematizer.get_refresh_by_id(
            self.active_refresh['id']
        )
        if refresh.status.value == RefreshStatus.IN_PROGRESS.value:
            self.schematizer.update_refresh(
                self.active_refresh['id'],
                RefreshStatus.FAILED,
                0
            )
            self.active_refresh['id'] = None
            self.active_refresh['pid'] = None

    def get_next_refresh(self):
        not_started_jobs = self.schematizer.get_refreshes_by_criteria(
            self.namespace,
            RefreshStatus.NOT_STARTED
        )
        paused_jobs = self.schematizer.get_refreshes_by_criteria(
            self.namespace,
            RefreshStatus.PAUSED
        )
        return self.determine_best_refresh(not_started_jobs, paused_jobs)

    def run(self):
        with ZKLock(name="refresh_manager", namespace=self.namespace):
            try:
                while True:
                    self.set_zombie_refresh_to_fail()
                    next_refresh = self.get_next_refresh()
                    if next_refresh and self._should_run_next_refresh(next_refresh):
                        self.setup_new_refresh(next_refresh)
                    if self._stopping:
                        break
                    time.sleep(SCHEMATIZER_POLL_FREQUENCY_SECONDS)
            finally:
                if self.active_refresh['pid'] is not None:
                    # When the manager goes down, the active refresh is paused.
                    os.kill(self.active_refresh['pid'], signal.SIGTERM)


if __name__ == '__main__':
    FullRefreshManager().start()
