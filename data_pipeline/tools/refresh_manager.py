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

import copy
import datetime
import os
import signal
import subprocess
import sys
import time
from optparse import OptionGroup

import psutil
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
INACTIVE_STATUSES = {RefreshStatus.NOT_STARTED, RefreshStatus.PAUSED}
DEFAULT_PER_SOURCE_THROUGHPUT_CAP = 50
DEFAULT_TOTAL_THROUGHPUT_CAP = 1000


class RefreshJob(object):
    def __init__(
        self,
        refresh_id,
        cap,
        priority,
        status,
        source,
        throughput=0,
        last_throughput=0,
        pid=None
    ):
        self.refresh_id = refresh_id
        self.cap = cap
        self.priority = priority
        self.status = status
        self.source = source
        self.throughput = throughput
        self.last_throughput = last_throughput
        self.pid = pid

    def should_modify(self):
        """A job should be modified rather than ran or paused if
        for some reason, a _running_ job's throughput has changed.
        If this happens for some reason, the job needs to be paused
        and restarted to get the proper throughput."""
        return (self.status == RefreshStatus.IN_PROGRESS and
                self.throughput and
                self.last_throughput and
                self.last_throughput != self.throughput)

    def should_run(self):
        return (self.status in INACTIVE_STATUSES and
                self.throughput and
                not self.last_throughput)

    def should_pause(self):
        return (self.status == RefreshStatus.IN_PROGRESS and
                not self.throughput)

    def is_active(self):
        return self.pid is not None and self.status not in COMPLETE_STATUSES


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

    @property
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
            '--cluster',
            dest='cluster',
            default='refresh_primary',
            help='Required: Specifies table cluster (default: %default).'
        )
        opt_group.add_option(
            '--database',
            dest='database',
            help='Specify the database to switch to after connecting to the '
                 'cluster.'
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
        if self.options.database is None:
            raise ValueError("--database is required to be defined")
        self.cluster = self.options.cluster
        self.database = self.options.database
        self.namespace = DBSourcedNamespace(
            cluster=self.cluster,
            database=self.database
        ).get_name()
        self.config_path = self.options.config_path
        self.dry_run = self.options.dry_run
        self.per_source_throughput_cap = self.options.per_source_throughput_cap
        self.total_throughput_cap = self.options.total_throughput_cap
        load_package_config(self.config_path)
        self.refresh_runner_path = self.get_refresh_runner_path()
        # Removing the cmd line arguments to prevent child process error.
        sys.argv = sys.argv[:1]

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
        if primary_keys:
            # The refresh runner doesn't like composite keys, and gets by just fine using only one of them
            primary_key = primary_keys[0]
        else:
            # Fallback in case we get bad data from the schematizer
            # TODO: DATAPIPE-1988
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
        job.pid = subprocess.Popen(args).pid

    def pause_job(self, job):
        # This signal will cause the refresh runner to update
        # the job to paused
        os.kill(job.pid, signal.SIGTERM)
        job.pid = None

    def modify_job(self, job):
        pid = job.pid
        self.pause_job(job)
        os.waitpid(pid)
        self.run_job(job)

    def set_zombie_refresh_to_fail(self, refresh_job):
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
            source = refresh_job.source
            del self.active_refresh_jobs[source]
        os.kill(current_pid, signal.SIGINT)

    def set_zombie_refreshes_to_fail(self):
        """The manager sometimes gets in to situations where
        a worker becomes a zombie but the refresh stays 'IN_PROGRESS'.
        For these situations we want to correct the refresh status to failed.
        """
        for source, refresh_job in self.active_refresh_jobs.iteritems():
            self.set_zombie_refresh_to_fail(refresh_job)

    def determine_refresh_candidates(self):
        refresh_candidates = self.refresh_queue.peek()
        self.log.debug("refresh_candidates: {}...".format(
            refresh_candidates
        ))
        return refresh_candidates

    def get_last_updated_timestamp(self, requests):
        if not requests:
            return self.last_updated_timestamp
        max_time = max([request.updated_at for request in requests])
        return int(
            (max_time - datetime.datetime(1970, 1, 1, tzinfo=max_time.tzinfo)).total_seconds()
        )

    def get_refresh_candidates(self, updated_refreshes):
        requests = [
            ref for ref in updated_refreshes if ref.status in INACTIVE_STATUSES
        ]
        self.log.debug("Sending {} refresh requests to refresh_queue".format(
            len(requests)
        ))
        self.refresh_queue.add_refreshes_to_queue(requests)
        return self.determine_refresh_candidates()

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
        """Rellocates available throughput to the currently running job
        in case more/less of it is available."""
        available_cap = self.get_cap()
        running_job = self.active_refresh_jobs[source]
        self.allocate_throughput_to_job(running_job, available_cap)

    def run_all_job_actions(self, to_run_jobs, to_modify_jobs, to_pause_jobs):
        for job in to_pause_jobs:
            self.pause_job(job)
        for job in to_modify_jobs:
            self.modify_job(job)
        for job in to_run_jobs:
            self.run_job(job)

    def delete_inactive_jobs(self):
        self.active_refresh_jobs = {
            source: refresh_job for source, refresh_job
            in self.active_refresh_jobs.items()
            if refresh_job.is_active()
        }

    def _should_run_new_job(self, refresh_candidate):
        source = refresh_candidate.source_name
        running_job = self.active_refresh_jobs.get(source, None)
        return (not running_job or
                running_job.status in COMPLETE_STATUSES or
                running_job.priority < refresh_candidate.priority)

    def _remove_running_job(self, source):
        running_job = self.active_refresh_jobs[source]
        self.total_throughput_being_used -= running_job.throughput
        # Pause the current running job so that it will stop running,
        # it will be added to the queue automatically next step
        if running_job.status not in COMPLETE_STATUSES:
            self.pause_job(running_job)
        del self.active_refresh_jobs[source]

    def update_running_jobs_with_refresh(self, refresh_candidate):
        if self._should_run_new_job(refresh_candidate):
            source = refresh_candidate.source_name
            new_refresh_job = RefreshJob(
                refresh_id=refresh_candidate.refresh_id,
                cap=(refresh_candidate.avg_rows_per_second_cap or
                     FullRefreshRunner.DEFAULT_AVG_ROWS_PER_SECOND_CAP),
                priority=refresh_candidate.priority,
                status=refresh_candidate.status,
                source=source
            )
            if source in self.active_refresh_jobs:
                self._remove_running_job(source)
            self.active_refresh_jobs[source] = new_refresh_job

    def update_running_jobs_with_refresh_candidates(self, refresh_candidates):
        for source, refresh in refresh_candidates.iteritems():
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

    def get_refreshes_of_statuses(self, statuses):
        refreshes = []
        for status in statuses:
            refreshes += self.schematizer.get_refreshes_by_criteria(
                self.namespace,
                status,
                updated_after=self.last_updated_timestamp
            )
        return refreshes

    def get_updated_refreshes(self):
        # We have to do it this way until the schematizer
        # is fixed for this call
        # (TODO: DATAPIPE-2074)
        statuses = list(INACTIVE_STATUSES)
        if self.active_refresh_jobs:
            statuses += list(COMPLETE_STATUSES)
            statuses.append(RefreshStatus.IN_PROGRESS)
        return self.get_refreshes_of_statuses(statuses)

    def active_job_has_invalid_status(self, active_job, updated_refresh):
        return (active_job and
                active_job.refresh_id == updated_refresh.refresh_id and
                updated_refresh.status == RefreshStatus.NOT_STARTED)

    def validate_running_job_status(self, active_job, updated_refresh, updated_refreshes):
        if self.active_job_has_invalid_status(active_job, updated_refresh):
            self.log.warning(
                "Discrepency found: active_job has not_started status in"
                " schematizer. Refresh ID: {} source_name: {}"
                ". If this warning is found shortly after the refresh is started"
                " then it's most likely not an issue. "
                "Removing from updated_refreshes list...".format(
                    active_job.refresh_id, updated_refresh.source_name
                )
            )
            updated_refreshes.remove(updated_refresh)

    def update_refreshes_status(self, updated_refreshes):
        for updated_refresh in copy.copy(updated_refreshes):
            source = updated_refresh.source_name
            active_job = self.active_refresh_jobs.get(source)
            if not active_job or active_job.refresh_id != updated_refresh.refresh_id:
                continue
            active_job.status = updated_refresh.status
            self.validate_running_job_status(active_job, updated_refresh, updated_refreshes)

    def step(self):
        self.set_zombie_refreshes_to_fail()
        updated_refreshes = self.get_updated_refreshes()
        self.last_updated_timestamp = self.get_last_updated_timestamp(
            updated_refreshes
        )
        if self.active_refresh_jobs:
            self.update_refreshes_status(updated_refreshes)
        refresh_candidates = self.get_refresh_candidates(updated_refreshes)
        self.update_running_jobs_with_refresh_candidates(refresh_candidates)
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
