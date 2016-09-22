# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
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
from yelp_servlib.config_util import load_package_config

from data_pipeline import __version__
from data_pipeline._namespace_util import DBSourcedNamespace
from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.copy_table_to_blackhole_table import FullRefreshRunner
from data_pipeline.zookeeper import ZKLock

SCHEMATIZER_POLL_FREQUENCY_SECONDS = 5
COMPLETE_STATUSES = {RefreshStatus.SUCCESS, RefreshStatus.FAILED}


class PriorityRefreshQueue:

    def __init__(self):
        self.queue = []
        self.ref = {}

    def update(self, jobs):
        for job in jobs:
            if job.refresh_id not in self.ref:
                self.queue.append(job.refresh_id)
            self.ref[job.refresh_id] = job

        # ternary sort in descending order, so that oldest updated jobs come first
        self.queue.sort(
            key=lambda id: self.ref[id].updated_at,
            reverse=True
        )
        # secondary sort in ascending order, so that paused jobs come first
        self.queue.sort(
            key=lambda id:
                (0 if self.ref[id].status == RefreshStatus.PAUSED else 1)
        )
        # primary sort in descending order, so that ones with the higest priority come first
        self.queue.sort(
            key=lambda id: self.ref[id].priority,
            reverse=True
        )

    def peek(self):
        if not self.queue:
            return None
        return self.ref[self.queue[0]]

    def pop(self):
        if not self.queue:
            return None
        item_id = self.queue.pop(0)
        item = self.ref[item_id]
        del self.ref[item_id]
        return item


class FullRefreshManager(BatchDaemon):

    def __init__(self):
        super(FullRefreshManager, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']
        self.active_refresh = {'id': None, 'pid': None}
        self._refresh_queue = PriorityRefreshQueue()
        self.last_updated_timestamp = None

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
        topics = self.schematizer.get_topics_by_criteria(
            source_name=refresh.source_name,
            namespace_name=refresh.namespace_name
        )
        topics.sort(key=lambda topic: topic.created_after)
        topic = topics[-1]
        primary_keys = self.schematizer.get_latest_schema_by_topic_name(topic.name).primary_keys
        if len(primary_keys):
            # The refresh runner doesn't like composite keys, and gets by just fine using only one of them
            primary = primary_keys[0]
        else:
            # Fallback in case we get bad data from the schematizer (command line default is 'id')
            primary = 'id'
        refresh_batch = FullRefreshRunner(
            refresh_id=self.active_refresh['id'],
            cluster=self.cluster,
            database=self.database,
            config_path=self.config_path,
            table_name=refresh.source_name,
            offset=refresh.offset,
            batch_size=refresh.batch_size,
            primary=primary,
            where_clause=refresh.filter_condition,
            dry_run=self.dry_run,
            avg_rows_per_second_cap=getattr(refresh, 'avg_rows_per_second_cap', None)
        )
        self.log.info(
            "Starting a batch table_name: {}, refresh_id: {}, worker_id: {}".format(
                refresh.source_name,
                self.active_refresh['id'],
                self.active_refresh['pid']
            )
        )
        refresh_batch.start()

    def setup_new_refresh(self, refresh):
        active_pid = self.active_refresh['pid']
        if active_pid is not None:
            # If we somehow have an active worker (replacing a lower priority refresh),
            # then we want to kill that worker, which will pause it.
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
        next_priority = next_refresh.priority
        current_priority = current_refresh.priority
        return (next_priority > current_priority or
                current_refresh.status in COMPLETE_STATUSES)

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
        if refresh.status == RefreshStatus.IN_PROGRESS:
            self.schematizer.update_refresh(
                self.active_refresh['id'],
                RefreshStatus.FAILED,
                0
            )
            self.active_refresh['id'] = None
            self.active_refresh['pid'] = None

    def determine_best_refresh(self, not_started_jobs, paused_jobs):
        jobs = not_started_jobs + paused_jobs

        self._refresh_queue.update(jobs)
        self.last_updated_timestamp = self.get_last_updated_timestamp(jobs)

        return self._refresh_queue.peek()

    def get_last_updated_timestamp(self, jobs):
        if not jobs:
            return self.last_updated_timestamp
        max_time = max([job.updated_at for job in jobs])
        return int((max_time - datetime.datetime(1970, 1, 1)).total_seconds())

    def get_next_refresh(self):
        not_started_jobs = self.schematizer.get_refreshes_by_criteria(
            self.namespace,
            RefreshStatus.NOT_STARTED,
            updated_after=self.last_updated_timestamp
        )
        paused_jobs = self.schematizer.get_refreshes_by_criteria(
            self.namespace,
            RefreshStatus.PAUSED,
            updated_after=self.last_updated_timestamp
        )
        return self.determine_best_refresh(not_started_jobs, paused_jobs)

    def run(self):
        with ZKLock(name="refresh_manager", namespace=self.namespace):
            try:
                while True:
                    self.set_zombie_refresh_to_fail()
                    next_refresh = self.get_next_refresh()
                    if next_refresh and self._should_run_next_refresh(next_refresh):
                        # Pop now that we know we're using it
                        next_refresh = self._refresh_queue.pop()
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
