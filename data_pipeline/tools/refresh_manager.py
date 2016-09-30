# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import datetime
import os
import signal
import subprocess
import sys
import time
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
PER_SOURCE_THROUGHPUT_CAP = 50
TOTAL_THROUGHPUT_CAP = 1000


class PriorityRefreshQueue:
    """
    PriorityQueue that sorts each source's queue by age, status and priority in that order,
    and then sorts sources by the top refresh in their queue with the same scheme.

    The only public ways to add/remove jobs from this queue are update and pop.

    We could implement this faster, but this is unnecessary as we have ample time between
    schematizer polls.
    """

    def __init__(self):
        self.source_to_refresh_queue = {}
        self.refresh_ref = {}

    def _add_job_to_queue(self, job):
        if job.refresh_id not in self.refresh_ref:
            if job.source_name not in self.source_to_refresh_queue:
                self.source_to_refresh_queue[job.source_name] = []
            self.source_to_refresh_queue[job.source_name].append(
                job.refresh_id
            )
        self.refresh_ref[job.refresh_id] = job

    def _top_refresh(self, source_name):
        return self.refresh_ref[
            self.source_to_refresh_queue[source_name][0]
        ]

    def _sort_refresh_queue(self, queue):
        # ternary sort in descending order, so that oldest created jobs come first
        queue.sort(
            key=lambda id: self.refresh_ref[id].created_at,
            reverse=True
        )
        # secondary sort in ascending order, so that paused jobs come first
        queue.sort(
            key=lambda id:
                (0 if self.refresh_ref[id].status == RefreshStatus.PAUSED else 1)
        )
        # primary sort in descending order, so that ones with the higest priority come first
        # we multiply by -1 instead of using reverse to keep order of previous sorts
        queue.sort(
            key=lambda id: (-1 * self.refresh_ref[id].priority)
        )

    def update(self, jobs):
        for job in jobs:
            self._add_job_to_queue(job)

        for _, queue in self.source_to_refresh_queue.iteritems():
            self._sort_refresh_queue(queue)

    def peek(self):
        return [
            (source_name, self._top_refresh(source_name))
            for source_name in self.source_to_refresh_queue.keys()
        ]

    def pop(self, source_name):
        item = self._top_refresh(source_name)
        refresh_id = self.source_to_refresh_queue[source_name].pop(0)
        del self.refresh_ref[refresh_id]
        if not self.source_to_refresh_queue[source_name]:
            del self.source_to_refresh_queue[source_name]
        return item


class FullRefreshManager(BatchDaemon):

    def __init__(self):
        super(FullRefreshManager, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']
        self.active_refreshes = {}
        self.total_throughput_being_used = 0
        self.refresh_queue = PriorityRefreshQueue()
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

    @cached_property
    def refresh_runner_path(self):
        return os.path.join(
            *(os.path.split(
                os.path.abspath(__file__)
             )[:-1] + tuple(["copy_table_to_blackhole_table.py"]))
        )

    def get_most_recent_topic_name(self, source_name, namespace_name):
        topics = self.schematizer.get_topics_by_criteria(
            source_name=source_name,
            namespace_name=namespace_name
        )
        topic = max(
            topics,
            key=lambda topic: topic.created_at
        )
        return topic.name

    def get_primary_from_topic_name(self, topic_name):
        primary_keys = self.schematizer.get_latest_schema_by_topic_name(
            topic_name
        ).primary_keys
        if len(primary_keys):
            # The refresh runner doesn't like composite keys, and gets by just fine using only one of them
            primary = primary_keys[0]
        else:
            # Fallback in case we get bad data from the schematizer (command line default is 'id')
            primary = 'id'
        return primary

    def get_refresh_args(self, job):
        refresh = job['object']
        args = [
            "python", self.refresh_runner_path,
            "--cluster={}".format(self.cluster),
            "--database={}".format(self.database),
            "--table-name={}".format(refresh.source_name),
            "--offset={}".format(refresh.offset),
            "--config-path={}".format(self.config_path),
            "--avg-rows-per-second-cap={}".format(job['throughput']),
            "--batch-size={}".format(refresh.batch_size),
            "--refresh-id={}".format(job['id'])
        ]
        if self.dry_run:
            args.append("--dry-run")
        if refresh.filter_condition:
            args.append("--where={}".format(
                refresh.filter_condition
            ))
        if self.options.verbose:
            vs = "v" * self.options.verbose
            args.append("-{}".format(vs))

        topic_name = self.get_most_recent_topic_name(
            refresh.source_name,
            refresh.namespace_name
        )
        primary = self.get_primary_from_topic_name(topic_name)
        args.append(
            "--primary={}".format(primary)
        )
        return args

    def run_job(self, job):
        if not job['last_throughput']:
            self.refresh_queue.pop(job['source'])

        args = self.get_refresh_args(job)
        self.log.info("Starting refresh with args: {}".format(
            args
        ))

        new_worker = subprocess.Popen(
            args
        )

        job['pid'] = new_worker.pid

    def pause_job(self, job):
        os.kill(job['pid'], signal.SIGTERM)
        job['pid'] = None

    def modify_job(self, job):
        self.pause_job(job)
        self.run_job(job)

    def set_zombie_refresh_to_fail(self, source, active_refresh):
        current_pid = active_refresh['pid']
        if current_pid is None:
            return

        p = psutil.Process(current_pid)
        if p.status() != psutil.STATUS_ZOMBIE:
            return

        refresh = self.schematizer.get_refresh_by_id(
            active_refresh['id']
        )
        if refresh.status == RefreshStatus.IN_PROGRESS:
            self.schematizer.update_refresh(
                active_refresh['id'],
                RefreshStatus.FAILED,
                0
            )
            self.remove_from_active_refreshes(source, active_refresh)

    def remove_from_active_refreshes(self, source, active_refresh):
        self.total_throughput_being_used -= active_refresh['throughput']
        self.active_refreshes[source].remove(active_refresh)
        if not self.active_refreshes[source]:
            del self.active_refreshes[source]

    def set_zombie_refreshes_to_fail(self):
        """The manager sometimes gets in to situations where
        a worker becomes a zombie but the refresh stays 'IN_PROGRESS'.
        For these situations we want to correct the refresh status to failed.
        """
        for source, source_active_refreshes in self.active_refreshes.iteritems():
            for active_refresh in source_active_refreshes:
                self.set_zombie_refresh_to_fail(source, active_refresh)

    def determine_best_refresh(self, not_started_jobs, paused_jobs):
        jobs = not_started_jobs + paused_jobs

        self.log.debug("Sending {} jobs to refresh_queue...".format(
            len(jobs)
        ))
        self.refresh_queue.update(jobs)
        self.last_updated_timestamp = self.get_last_updated_timestamp(jobs)

        next_refresh_set = self.refresh_queue.peek()
        self.log.debug("next_refresh_set: {}...".format(
            next_refresh_set
        ))
        return next_refresh_set

    def get_last_updated_timestamp(self, jobs):
        if not jobs:
            return self.last_updated_timestamp
        max_time = max([job.updated_at for job in jobs])
        return int(
            (max_time - datetime.datetime(1970, 1, 1, tzinfo=max_time.tzinfo)).total_seconds()
        ) + 1

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

    def get_cap(self):
        cap_left = TOTAL_THROUGHPUT_CAP - self.total_throughput_being_used
        return min(cap_left, PER_SOURCE_THROUGHPUT_CAP)

    def top_refresh(self, source_name):
        return self.active_refreshes[source_name][0]

    def construct_running_refreshes(self, source, refresh_to_add=None):
        source_refreshes = self.active_refreshes.get(source, [])
        for refresh in source_refreshes:
            refresh['status'] = self.schematizer.get_refresh_by_id(
                refresh['id']
            ).status
        if refresh_to_add:
            source_refreshes.append({
                'id': refresh_to_add.refresh_id,
                'cap': (refresh_to_add.avg_rows_per_second_cap or
                    FullRefreshRunner.DEFAULT_AVG_ROWS_PER_SECOND_CAP),
                'throughput': 0,
                'pid': None,
                'priority': refresh_to_add.priority,
                'status': refresh_to_add.status,
                'source': source,
                'object': refresh_to_add
            })
        self.sort_source_refreshes(source_refreshes)
        # In case active_refreshes doesn't exist for the source
        self.active_refreshes[source] = source_refreshes

    def sort_source_refreshes(self, source_refreshes):
        # Ternary sort to distinguish between running and not running
        # (running has priority)
        source_refreshes.sort(
            key=lambda refresh:
                (0 if refresh['status'] == RefreshStatus.IN_PROGRESS \
                    else 1)
        )

        # Secondary sort on priority
        source_refreshes.sort(
            key=lambda refresh:
                refresh['priority'],
            reverse=True
        )

        # Sort to put all completed refreshes at the end
        source_refreshes.sort(
            key=lambda refresh:
                (1 if refresh['status'] in COMPLETE_STATUSES \
                    else 0)
        )

    def allocate_throughput_to_refreshes(self, source_refreshes, cap):
        delta_throughput = 0
        for refresh in source_refreshes:
            old_throughput = refresh['throughput']
            refresh['last_throughput'] = old_throughput
            new_throughput = min(cap, refresh['cap'])
            cap -= new_throughput
            delta_throughput += (old_throughput-new_throughput)
            refresh['throughput'] = new_throughput
        self.total_throughput_being_used += delta_throughput

    def _should_run(self, refresh):
        return not refresh['last_throughput'] and refresh['throughput']

    def _should_modify(self, refresh):
        return (refresh['last_throughput'] and
                refresh['throughput'] and
                refresh['throughput'] != refresh['last_throughput']
               )

    def _should_pause(self, refresh):
        return refresh['last_throughput'] and not refresh['throughput']

    def update_job_actions(self, source_refreshes):
        self.to_run_jobs = [
            refresh for refresh in source_refreshes \
                if self._should_run(refresh)
        ]
        self.to_modify_jobs = [
            refresh for refresh in source_refreshes \
                if self._should_modify(refresh)
        ]
        self.to_pause_jobs = [
            refresh for refresh in source_refreshes \
                if self._should_pause(refresh)
        ]

    def reallocate_for_source(self, source):
        cap = self.get_cap()
        source_refreshes = self.active_refreshes[source]
        self.allocate_throughput_to_refreshes(source_refreshes, cap)
        self.update_job_actions(source_refreshes)

    def source_in_refresh_set(self, target_source, next_refresh_set):
        for source, _ in next_refresh_set:
            if source == target_source:
                return True
        return False

    def run_all_job_actions(self):
        for job in self.to_pause_jobs:
            self.pause_job(job)
        for job in self.to_modify_jobs:
            self.modify_job(job)
        for job in self.to_run_jobs:
            self.run_job(job)

    def delete_inactive_and_failed_jobs(self):
        for source, active_refreshes in self.active_refreshes.items():
            for job in active_refreshes:
                if job['pid'] is None or job['status'] == RefreshStatus.FAILED:
                    active_refreshes.remove(job)
            if not self.active_refreshes[source]:
                del self.active_refreshes[source]

    def reallocate_throughput(self, next_refresh_set):
        self.to_run_jobs = []
        self.to_modify_jobs = []
        self.to_pause_jobs = []
        next_refresh_set += [(source, None) for source in self.active_refreshes.keys()
            if not self.source_in_refresh_set(source, next_refresh_set)]
        for source, refresh in next_refresh_set:
            self.log.debug("Constructing running refreshes for {}...".format(
                source
            ))
            self.construct_running_refreshes(source, refresh)
        source_names = self.sort_sources_by_top_refresh()
        for source in source_names:
            self.log.debug("Reallocating for {}...".format(
                source
            ))
            self.reallocate_for_source(source)
        self.log.debug("to_run: {}\n\nto_modify: {}\n\nto_pause: {}...".format(
            self.to_run_jobs, self.to_modify_jobs, self.to_pause_jobs
        ))
        self.run_all_job_actions()

    def sort_sources_by_top_refresh(self):
        source_names = copy.copy(self.active_refreshes.keys())
        # Ternary sort to distinguish between running and not running
        # (running has priority)
        source_names.sort(
            key=lambda name:
                (0 if self.top_refresh(name)['status'] == RefreshStatus.IN_PROGRESS \
                    else 1)
        )

        # Secondary sort on priority
        source_names.sort(
            key=lambda name:
                self.top_refresh(name)['priority'],
            reverse=True
        )

        # Sort to put all completed refreshes at the end
        source_names.sort(
            key=lambda name:
                (1 if self.top_refresh(name)['status'] in COMPLETE_STATUSES \
                    else 0)
        )
        return source_names

    def pause_all_running_jobs(self):
        for _, active_refreshes in self.active_refreshes.items():
            for job in active_refreshes:
                # Need to check if there's an actual pid in case
                # interruption while we are adding jobs
                if job['pid'] is not None:
                    self.pause_job(job)

    def run(self):
        with ZKLock(name="refresh_manager", namespace=self.namespace):
            try:
                while True:
                    self.set_zombie_refreshes_to_fail()
                    next_refresh_set = self.get_next_refresh()
                    self.reallocate_throughput(next_refresh_set)
                    self.delete_inactive_and_failed_jobs()
                    if self._stopping:
                        break
                    self.log.debug(
                        "State of active_refreshes: {}".format(
                            self.active_refreshes
                        )
                    )
                    time.sleep(SCHEMATIZER_POLL_FREQUENCY_SECONDS)
            finally:
                self.pause_all_running_jobs()


if __name__ == '__main__':
    FullRefreshManager().start()
