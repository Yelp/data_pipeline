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

    def get_most_recent_topic_name(self, source_name, namespace_name):
        # TODO: DATAPIPE-1866
        # This should realistically be something like get_latest_topic_by_names
        # or something similar
        topics = self.schematizer.get_topics_by_criteria(
            source_name=source_name,
            namespace_name=namespace_name
        )
        topic = max(
            topics,
            key=lambda topic: topic.created_at
        )
        return topic.name

    def get_primary_key_from_topic_name(self, topic_name):
        primary_keys = self.schematizer.get_latest_schema_by_topic_name(
            topic_name
        ).primary_keys
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

        topic_name = self.get_most_recent_topic_name(
            refresh.source_name,
            refresh.namespace_name
        )
        primary_key = self.get_primary_key_from_topic_name(topic_name)
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

    def set_zombie_refresh_to_fail(self, source, active_refresh_job):
        current_pid = active_refresh_job.pid
        if current_pid is None:
            return

        p = psutil.Process(current_pid)
        if p.status() != psutil.STATUS_ZOMBIE:
            return

        refresh = self.schematizer.get_refresh_by_id(
            active_refresh_job.refresh_id
        )
        if refresh.status == RefreshStatus.IN_PROGRESS:
            self.schematizer.update_refresh(
                refresh_id=active_refresh_job.refresh_id,
                status=RefreshStatus.FAILED,
                offset=0
            )
            self.remove_from_active_refresh_jobs(source, active_refresh_job)
        # This signal will cause the refresh runner to update the job as
        # failed
        os.kill(current_pid, signal.SIGINT)

    def remove_from_active_refresh_jobs(self, source, active_refresh_job):
        self.total_throughput_being_used -= active_refresh_job.throughput
        self.active_refresh_jobs[source].remove(active_refresh_job)
        if not self.active_refresh_jobs[source]:
            del self.active_refresh_jobs[source]

    def set_zombie_refreshes_to_fail(self):
        """The manager sometimes gets in to situations where
        a worker becomes a zombie but the refresh stays 'IN_PROGRESS'.
        For these situations we want to correct the refresh status to failed.
        """
        for source, source_active_refresh_jobs in self.active_refresh_jobs.iteritems():
            for active_refresh_job in source_active_refresh_jobs:
                self.set_zombie_refresh_to_fail(source, active_refresh_job)

    def determine_next_refresh_set(self, requests):
        self.log.debug("Sending {} refresh requests to refresh_queue".format(
            len(requests)
        ))

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
        self.refresh_queue.update(requests)
        self.last_updated_timestamp = self.get_last_updated_timestamp(requests)
        return self.determine_next_refresh_set(requests)

    def get_cap(self):
        cap_left = self.total_throughput_cap - self.total_throughput_being_used
        return min(cap_left, self.per_source_throughput_cap)

    def top_refresh_job(self, source_name):
        return self.active_refresh_jobs[source_name][0]

    def construct_running_refreshes(self, source, refresh_to_add=None):
        source_refresh_jobs = self.active_refresh_jobs.get(source, [])
        for refresh_job in source_refresh_jobs:
            refresh_job.status = self.schematizer.get_refresh_by_id(
                refresh_job.refresh_id
            ).status
        if refresh_to_add:
            source_refresh_jobs.append(RefreshJob(
                refresh_id=refresh_to_add.refresh_id,
                cap=(refresh_to_add.avg_rows_per_second_cap or
                     FullRefreshRunner.DEFAULT_AVG_ROWS_PER_SECOND_CAP),
                priority=refresh_to_add.priority,
                status=refresh_to_add.status,
                source=source
            ))
        source_refresh_jobs = self.sort_source_refresh_jobs(source_refresh_jobs)
        # In case active_refresh_jobs doesn't exist for the source
        self.active_refresh_jobs[source] = source_refresh_jobs
        if not source_refresh_jobs:
            del self.active_refresh_jobs[source]

    def _sort_by_running_first(
        self, l,
        get_refresh_job_fn=lambda job: job
    ):
        return sorted(
            l,
            key=lambda item:
                (0 if get_refresh_job_fn(item).status == RefreshStatus.IN_PROGRESS
                 else 1)
        )

    def _sort_by_descending_priority(
        self, l,
        get_refresh_job_fn=lambda job: job
    ):
        return sorted(
            l,
            key=lambda item:
                get_refresh_job_fn(item).priority,
            reverse=True
        )

    def _filter_out_completed_refresh_jobs(self, refresh_jobs):
        return filter(
            lambda job: job.status not in COMPLETE_STATUSES,
            refresh_jobs
        )

    def sort_source_refresh_jobs(self, source_refresh_jobs):
        source_refresh_jobs = self._sort_by_running_first(source_refresh_jobs)
        source_refresh_jobs = self._sort_by_descending_priority(source_refresh_jobs)
        return self._filter_out_completed_refresh_jobs(source_refresh_jobs)

    def sort_sources_by_top_refresh(self):
        # We sort our sources by the top refresh as it
        # is a fairly good way of sorting the sources in general
        # as most sources are going to run exactly one refresh
        source_names = self.active_refresh_jobs.keys()

        def get_refresh_job_fn(name):
            return self.top_refresh_job(name)

        source_names = self._sort_by_running_first(
            source_names,
            get_refresh_job_fn=get_refresh_job_fn
        )
        return self._sort_by_descending_priority(
            source_names,
            get_refresh_job_fn=get_refresh_job_fn
        )

    def allocate_throughput_to_refreshes(self, source_refresh_jobs, cap):
        delta_throughput = 0
        for refresh_job in source_refresh_jobs:
            old_throughput = refresh_job.throughput
            refresh_job.last_throughput = old_throughput
            new_throughput = min(cap, refresh_job.cap)
            cap -= new_throughput
            delta_throughput += (new_throughput - old_throughput)
            refresh_job.throughput = new_throughput
        self.total_throughput_being_used += delta_throughput

    def _should_run(self, refresh_job):
        return not refresh_job.last_throughput and refresh_job.throughput

    def _should_modify(self, refresh_job):
        return (refresh_job.status == RefreshStatus.IN_PROGRESS and
                refresh_job.throughput and
                refresh_job.throughput != refresh_job.last_throughput
                )

    def _should_pause(self, refresh_job):
        return refresh_job.last_throughput and not refresh_job.throughput

    def update_job_actions(self):
        self.to_run_jobs = []
        self.to_modify_jobs = []
        self.to_pause_jobs = []
        for _, source_refresh_jobs in self.active_refresh_jobs.iteritems():
            self.to_run_jobs += [
                refresh_job for refresh_job in source_refresh_jobs
                if self._should_run(refresh_job)
            ]
            self.to_modify_jobs += [
                refresh_job for refresh_job in source_refresh_jobs
                if self._should_modify(refresh_job)
            ]
            self.to_pause_jobs += [
                refresh_job for refresh_job in source_refresh_jobs
                if self._should_pause(refresh_job)
            ]
        self.log.debug("to_run: {}".format(self.to_run_jobs))
        self.log.debug("to_modify: {}".format(self.to_modify_jobs))
        self.log.debug("to_pause: {}".format(self.to_pause_jobs))

    def reallocate_for_source(self, source):
        cap = self.get_cap()
        source_refresh_jobs = self.active_refresh_jobs[source]
        self.allocate_throughput_to_refreshes(source_refresh_jobs, cap)

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

    def delete_inactive_jobs(self):
        jobs_to_remove = []
        for source, active_refresh_jobs in self.active_refresh_jobs.iteritems():
            for refresh_job in active_refresh_jobs:
                if refresh_job.pid is None:
                    jobs_to_remove.append((source, refresh_job))
        for source, job in jobs_to_remove:
            self.active_refresh_jobs[source].remove(job)
            if not self.active_refresh_jobs[source]:
                del self.active_refresh_jobs[source]

    def add_sources_from_active_refresh_jobs(self, next_refresh_set):
        for source in self.active_refresh_jobs.keys():
            if source not in next_refresh_set:
                next_refresh_set[source] = None

    def add_next_refresh_set_to_active_refresh_jobs(self, next_refresh_set):
        self.add_sources_from_active_refresh_jobs(next_refresh_set)
        for source, refresh in next_refresh_set.iteritems():
            self.log.debug("Constructing running refreshes for {}...".format(
                source
            ))
            self.construct_running_refreshes(source, refresh)

    def reallocate_throughput(self):
        source_names = self.sort_sources_by_top_refresh()
        for source in source_names:
            self.log.debug("Reallocating for {}...".format(
                source
            ))
            self.reallocate_for_source(source)

    def pause_all_running_jobs(self):
        for _, active_refresh_jobs in self.active_refresh_jobs.items():
            for job in active_refresh_jobs:
                # Need to check if there's an actual pid in case
                # interruption while we are adding jobs
                if job.pid is not None:
                    self.pause_job(job)

    def step(self):
        self.set_zombie_refreshes_to_fail()
        next_refresh_set = self.get_next_refresh()
        self.add_next_refresh_set_to_active_refresh_jobs(next_refresh_set)
        self.reallocate_throughput()
        self.update_job_actions()
        self.run_all_job_actions()
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
