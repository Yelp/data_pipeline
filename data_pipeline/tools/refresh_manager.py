# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import os
import signal
import subprocess
import sys
import time
from optparse import OptionGroup

import attr
import psutil
from cached_property import cached_property
from yelp_batch import BatchDaemon
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure

from data_pipeline import __version__
from data_pipeline._namespace_util import DBSourcedNamespace
from data_pipeline.helpers.priority_refresh_queue import PriorityRefreshQueue
from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.servlib.config_util import load_package_config
from data_pipeline.tools.copy_table_to_blackhole_table import FullRefreshRunner
from data_pipeline.zookeeper import ZKLock

SCHEMATIZER_POLL_FREQUENCY_SECONDS = 5
COMPLETE_STATUSES = {RefreshStatus.SUCCESS, RefreshStatus.FAILED}
DEFAULT_PER_SOURCE_THROUGHPUT_CAP = 50
DEFAULT_TOTAL_THROUGHPUT_CAP = 1000


@attr.s
class RefreshJob(object):
    refresh_id = attr.ib()
    cap = attr.ib()
    priority = attr.ib()
    status = attr.ib()
    source = attr.ib()
    throughput = attr.ib(default=0)
    last_throughput = attr.ib(default=0)
    pid = attr.ib(default=None)

    def should_modify(self):
        return (self.status == RefreshStatus.IN_PROGRESS and
                self.last_throughput and
                self.last_throughput != self.throughput)

    def should_run(self):
        return (self.status != RefreshStatus.IN_PROGRESS and
                self.throughput and
                not self.last_throughput)

    def should_pause(self):
        return (self.status != RefreshStatus.IN_PROGRESS and
                not self.throughput)


class FullRefreshManager(BatchDaemon):
    """
    A Daemon that monitors all refreshes on a given namespace that need to be run
    (getting this info from the schematizer), and allocates throughput on a per-source basis
    to do this without taxing other functionality. It runs the refreshes using FullRefreshRunner
    subprocesses. To create a refresh request, use FullRefreshJob.
    """

    def __init__(self):
        super(FullRefreshManager, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']
        self.active_refresh_jobs = {}
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
        opt_group.add_option(
            '--per-source-throughput-cap',
            default=DEFAULT_PER_SOURCE_THROUGHPUT_CAP,
            dest="per_source_throughput_cap",
            help="The cap that each source within the namespace is given. Any source in this "
                 "namespace cannot have a throughput higher than this cap. (default: %default)"
        )
        opt_group.add_option(
            '--total-throughput-cap',
            default=DEFAULT_TOTAL_THROUGHPUT_CAP,
            dest="total_throughput_cap",
            help="The cap that the namespace gives in general. The cumulative running refreshes "
                 "cannot have a throughput higher than this number. (default: %default)"
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
        self.per_source_throughput_cap = self.options.per_source_throughput_cap
        self.total_throughput_cap = self.options.total_throughput_cap
        load_package_config(self.config_path)
        self.refresh_runner_path = self.get_refresh_runner_path()
        # Removing the cmd line arguments to prevent child process error.
        sys.argv = sys.argv[:1]

    def _set_cluster_and_database(self):
        namespace_info = DBSourcedNamespace.create_from_namespace_name(self.namespace)
        self.cluster = namespace_info.cluster
        self.database = namespace_info.database

    def get_refresh_runner_path(self):
        return os.path.join(
            *(os.path.split(
                os.path.abspath(__file__)
            )[:-1] + ("copy_table_to_blackhole_table.py",))
        )

    def get_most_recent_topic(self, source_name, namespace_name):
        # TODO: DATAPIPE-1866
        # This should realistically be something like get_latest_topic_by_names
        # or something similar
        topics = self.schematizer.get_topics_by_criteria(
            source_name=source_name,
            namespace_name=namespace_name
        )
        return max(
            topics,
            key=lambda topic: topic.created_at
        )

    def get_primary_key_from_topic(self, topic):
        primary_keys = topic.primary_keys
        if len(primary_keys):
            # The refresh runner doesn't like composite keys, and gets by just fine using only one of them
            primary_key = primary_keys[0]
        else:
            # Fallback in case we get bad data from the schematizer (command line default is 'id')
            primary_key = 'id'
        return primary_key

    def get_refresh_args(self, job):
        # Important to grab fresh refresh here to get offset in case of
        # a paused job
        refresh = self.schematizer.get_refresh_by_id(
            job.refresh_id
        )
        args = [
            "python", self.refresh_runner_path,
            "--cluster={}".format(self.cluster),
            "--database={}".format(self.database),
            "--table-name={}".format(refresh.source_name),
            "--offset={}".format(refresh.offset),
            "--config-path={}".format(self.config_path),
            "--avg-rows-per-second-cap={}".format(job.throughput),
            "--batch-size={}".format(refresh.batch_size),
            "--refresh-id={}".format(job.refresh_id)
        ]
        if self.dry_run:
            args.append("--dry-run")
        if refresh.filter_condition:
            args.append("--where=\"{}\"".format(
                refresh.filter_condition
            ))
        if self.options.verbose:
            vs = "v" * self.options.verbose
            args.append("-{}".format(vs))

        topic = self.get_most_recent_topic(
            refresh.source_name,
            refresh.namespace_name
        )
        primary_key = self.get_primary_key_from_topic(topic)
        args.append(
            "--primary-key={}".format(primary_key)
        )
        return args

    def run_job(self, job):
        if not job.last_throughput:
            self.refresh_queue.pop(job.source)

        args = self.get_refresh_args(job)
        self.log.info("Starting refresh with args: {}".format(args))

        new_worker = subprocess.Popen(args)

        job.pid = new_worker.pid

    def pause_job(self, job):
        if job.status not in COMPLETE_STATUSES:
            # This signal will cause the refresh runner to update
            # the job to paused
            os.kill(job.pid, signal.SIGTERM)
        job.pid = None

    def modify_job(self, job):
        self.pause_job(job)
        self.run_job(job)

    def set_zombie_refresh_to_fail(self, refresh_job):
        source = refresh_job.source
        current_pid = refresh_job.pid
        if current_pid is None:
            return

        p = psutil.Process(current_pid)
        if p.status() != psutil.STATUS_ZOMBIE:
            return

        refresh = self.schematizer.get_refresh_by_id(
            refresh_job.refresh_id
        )
        if refresh.status == RefreshStatus.IN_PROGRESS:
            # Must update manually (not relying on the signal),
            # as the process may not properly handle the signal
            # if it's a zombie
            self.schematizer.update_refresh(
                refresh_id=refresh_job.refresh_id,
                status=RefreshStatus.FAILED,
                offset=0
            )
            del self.active_refresh_jobs[source]
        os.kill(current_pid, signal.SIGINT)

    def set_zombie_refreshes_to_fail(self):
        """The manager sometimes gets in to situations where
        a worker becomes a zombie but the refresh stays 'IN_PROGRESS'.
        For these situations we want to correct the refresh status to failed.
        """
        for source, refresh_job in self.active_refresh_jobs.iteritems():
            self.set_zombie_refresh_to_fail(refresh_job)

    def determine_next_refresh_set(self):
        next_refresh_set = self.refresh_queue.peek()
        self.log.debug("next_refresh_set: {}...".format(
            next_refresh_set
        ))
        return next_refresh_set

    def get_last_updated_timestamp(self, requests):
        if not requests:
            return self.last_updated_timestamp
        max_time = max([request.updated_at for request in requests])
        return int(
            (max_time - datetime.datetime(1970, 1, 1, tzinfo=max_time.tzinfo)).total_seconds()
        )

    def get_next_refresh(self):
        not_started_requests = self.schematizer.get_refreshes_by_criteria(
            self.namespace,
            RefreshStatus.NOT_STARTED,
            updated_after=self.last_updated_timestamp
        )
        paused_requests = self.schematizer.get_refreshes_by_criteria(
            self.namespace,
            RefreshStatus.PAUSED,
            updated_after=self.last_updated_timestamp
        )
        requests = not_started_requests + paused_requests
        self.log.debug("Sending {} refresh requests to refresh_queue".format(
            len(requests)
        ))
        self.refresh_queue.add_refreshes_to_queue(requests)
        self.last_updated_timestamp = self.get_last_updated_timestamp(requests)
        return self.determine_next_refresh_set()

    def get_cap(self):
        cap_left = self.total_throughput_cap - self.total_throughput_being_used
        return min(cap_left, self.per_source_throughput_cap)

    def _sort_by_running_first(self, sources):
        return sorted(
            sources,
            key=lambda source:
                (0 if self.active_refresh_jobs[source].status == RefreshStatus.IN_PROGRESS
                 else 1)
        )

    def _sort_by_descending_priority(self, sources):
        return sorted(
            sources,
            key=lambda source:
                self.active_refresh_jobs[source].priority,
            reverse=True
        )

    def sort_sources(self):
        source_names = self.active_refresh_jobs.keys()
        source_names = self._sort_by_running_first(
            source_names
        )
        return self._sort_by_descending_priority(
            source_names
        )

    def allocate_throughput_to_job(self, job, cap):
        job.last_throughput = job.throughput
        job.throughput = min(cap, job.cap)
        self.total_throughput_being_used += (
            job.throughput - job.last_throughput
        )

    def update_job_actions(self):
        to_run_jobs = []
        to_modify_jobs = []
        to_pause_jobs = []
        for source, refresh_job in self.active_refresh_jobs.iteritems():
            if refresh_job.should_run():
                to_run_jobs.append(refresh_job)
            elif refresh_job.should_modify():
                to_modify_jobs.append(refresh_job)
            elif refresh_job.should_pause():
                to_pause_jobs.append(refresh_job)
        self.log.debug("to_run: {}".format(to_run_jobs))
        self.log.debug("to_modify: {}".format(to_modify_jobs))
        self.log.debug("to_pause: {}".format(to_pause_jobs))
        return to_run_jobs, to_modify_jobs, to_pause_jobs

    def reallocate_for_source(self, source):
        cap = self.get_cap()
        job = self.active_refresh_jobs[source]
        self.allocate_throughput_to_job(job, cap)

    def run_all_job_actions(self, to_run_jobs, to_modify_jobs, to_pause_jobs):
        for job in to_pause_jobs:
            self.pause_job(job)
        for job in to_modify_jobs:
            self.modify_job(job)
        for job in to_run_jobs:
            self.run_job(job)

    def delete_inactive_jobs(self):
        sources_to_remove = []
        for source, refresh_job in self.active_refresh_jobs.iteritems():
            if refresh_job.pid is None or refresh_job.status in COMPLETE_STATUSES:
                sources_to_remove.append(source)
        for source in sources_to_remove:
            del self.active_refresh_jobs[source]

    def _should_replace_running_job(self, candidate_job):
        source = candidate_job.source
        running_job = self.active_refresh_jobs[source]
        return (running_job.status in COMPLETE_STATUSES or
                running_job.priority < candidate_job.priority)

    def _remove_running_job(self, source):
        running_job = self.active_refresh_jobs[source]
        self.total_throughput_being_used -= running_job.throughput
        self.pause_job(running_job)
        del self.active_refresh_jobs[source]

    def update_running_jobs_with_refresh(self, refresh_to_add):
        source = refresh_to_add.source_name
        new_refresh_job = RefreshJob(
            refresh_id=refresh_to_add.refresh_id,
            cap=(refresh_to_add.avg_rows_per_second_cap or
                 FullRefreshRunner.DEFAULT_AVG_ROWS_PER_SECOND_CAP),
            priority=refresh_to_add.priority,
            status=refresh_to_add.status,
            source=source
        )
        if source not in self.active_refresh_jobs:
            self.active_refresh_jobs[source] = new_refresh_job
        elif self._should_replace_running_job(new_refresh_job):
            self._remove_running_job(source)
            self.active_refresh_jobs[source] = new_refresh_job

    def update_running_jobs_with_refresh_set(self, refresh_set):
        for source, refresh in refresh_set.iteritems():
            self.log.debug("Constructing running refreshes for {}...".format(
                source
            ))
            self.update_running_jobs_with_refresh(refresh)

    def reallocate_throughput(self):
        source_names = self.sort_sources()
        for source in source_names:
            self.log.debug("Reallocating for {}...".format(
                source
            ))
            self.reallocate_for_source(source)

    def pause_all_running_jobs(self):
        for _, job in self.active_refresh_jobs.iteritems():
            # Need to check if there's an actual pid in case
            # interruption while we are adding jobs
            if job.pid is not None:
                self.pause_job(job)

    def reset_running_refresh_statuses(self):
        for source, running_job in self.active_refresh_jobs.iteritems():
            running_job.status = self.schematizer.get_refresh_by_id(
                running_job.refresh_id
            ).status

    def step(self):
        self.set_zombie_refreshes_to_fail()
        next_refresh_set = self.get_next_refresh()
        self.reset_running_refresh_statuses()
        self.update_running_jobs_with_refresh_set(next_refresh_set)
        self.reallocate_throughput()
        to_run_jobs, to_modify_jobs, to_pause_jobs = self.update_job_actions()
        self.run_all_job_actions(to_run_jobs, to_modify_jobs, to_pause_jobs)
        self.delete_inactive_jobs()

    def run(self):
        with ZKLock(name="refresh_manager", namespace=self.namespace):
            try:
                while True:
                    self.step()
                    if self._stopping:
                        break
                    self.log.debug(
                        "State of active_refresh_jobs: {}".format(
                            self.active_refresh_jobs
                        )
                    )
                    time.sleep(SCHEMATIZER_POLL_FREQUENCY_SECONDS)
            finally:
                self.pause_all_running_jobs()


if __name__ == '__main__':
    FullRefreshManager().start()
