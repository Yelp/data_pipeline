# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import signal
import sys
import time
from multiprocessing import Process
from optparse import OptionGroup

import psutil
from enum import Enum
from yelp_batch import BatchDaemon
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure
from yelp_servlib.config_util import load_default_config

from data_pipeline.batch.copy_table_to_blackhole_table import FullRefreshRunner
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


SCHEMATIZER_POLL_FREQUENCY_SECONDS = 5


class Priority(Enum):
    LOW = 25
    MEDIUM = 50
    HIGH = 75
    MAX = 100


class FullRefreshManager(BatchDaemon):

    def __init__(self):
        super(FullRefreshManager, self).__init__()
        self.notify_emails = ['bam+batch@yelp.com']
        self.active_refresh = {'id': None, 'pid': None}
        load_default_config('/nail/srv/configs/data_pipeline_tools.yaml')
        self.schematizer = get_schematizer()

    @batch_command_line_options
    def define_options(self, option_parser):
        opt_group = OptionGroup(option_parser, 'Full Refresh Manager Options')

        opt_group.add_option(
            '--namespace-name',
            dest='namespace',
            help='Name of the namespace this refresh manager will handle.'
        )
        opt_group.add_option(
            '--config-path',
            dest='config_path',
            default='/nail/srv/configs/data_pipeline_tools.yaml',
            help='Config file path for the refresh manager. '
                 '(default: %default)'
        )
        return opt_group

    @batch_configure
    def _init_global_state(self):
        self.namespace_name = self.options.namespace
        if self.namespace_name:
            names = self.namespace_name.split('.')
            self.cluster = names[0]
            self.database = None
            if len(names) == 2:
                self.database = names[1]
        self.config_path = self.options.config_path
        # Removing the cmd line arguments to prevent child process error.
        sys.argv = sys.argv[:1]

    def _begin_refresh_job(self, refresh):
        primary_key = 'id'
        refresh_batch = FullRefreshRunner(
            self.active_refresh['id'],
            self.cluster,
            self.database,
            self.config_path,
            refresh.source.name,
            refresh.offset,
            refresh.batch_size,
            primary_key,
            refresh.filter_condition,
        )
        refresh_batch.start()

    def setup_new_refresh(self, refresh):
        active_pid = self.active_refresh['pid']
        if active_pid is not None:
            os.kill(active_pid, signal.SIGTERM)
        new_worker = Process(
            target=self._begin_refresh_job,
            args=(refresh,)
        )
        self.active_refresh['id'] = refresh.refresh_id
        new_worker.start()
        self.active_refresh['pid'] = new_worker.pid

    def process_next_refresh(self, next_refresh):
        if not self.active_refresh['id']:
            self.setup_new_refresh(next_refresh)
        else:
            current_refresh = self.schematizer.get_refresh_by_id(
                self.active_refresh['id']
            )
            next_priority = Priority[next_refresh.priority].value
            current_priority = Priority[current_refresh.priority].value
            complete_statuses = set(['SUCCESS', 'FAILED'])
            if next_priority > current_priority:
                self.setup_new_refresh(next_refresh)
            elif current_refresh.status in complete_statuses:
                self.setup_new_refresh(next_refresh)

    def determine_best_refresh(self, refresh_list_a, refresh_list_b):
        if refresh_list_a and refresh_list_b:
            a_priority = Priority[refresh_list_a[0].priority].value
            b_priority = Priority[refresh_list_b[0].priority].value
            if a_priority > b_priority:
                return refresh_list_a[0]
            else:
                return refresh_list_b[0]
        if refresh_list_a:
            return refresh_list_a[0]
        if refresh_list_b:
            return refresh_list_b[0]
        return None

    def handle_zombie_refreshes(self):
        current_pid = self.active_refresh['pid']
        if current_pid is not None:
            p = psutil.Process(current_pid)
            if p.status() == psutil.STATUS_ZOMBIE:
                refresh = self.schematizer.get_refresh_by_id(
                    self.active_refresh['id']
                )
                if refresh.status == 'IN_PROGRESS':
                    self.schematizer.update_refresh(
                        self.active_refresh['id'],
                        'FAILED',
                        0
                    )
                    self.active_refresh['id'] = None
                    self.active_refresh['pid'] = None

    def get_next_refresh(self):
        not_started_jobs = self.schematizer.get_refreshes_by_criteria(
            self.namespace_name,
            'NOT_STARTED'
        )
        paused_jobs = self.schematizer.get_refreshes_by_criteria(
            self.namespace_name,
            'PAUSED'
        )
        return self.determine_best_refresh(not_started_jobs, paused_jobs)

    def run(self):
        try:
            while True:
                self.handle_zombie_refreshes()
                next_refresh = self.get_next_refresh()
                if next_refresh:
                    self.process_next_refresh(next_refresh)
                if self._stopping:
                    break
                time.sleep(SCHEMATIZER_POLL_FREQUENCY_SECONDS)
        finally:
            if self.active_refresh['pid'] is not None:
                # When the manager goes down, the active refresh is paused.
                os.kill(self.active_refresh['pid'], signal.SIGTERM)


if __name__ == '__main__':
    FullRefreshManager().start()
