# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import time
import yelp_conn

from datetime import timedelta
from datetime import datetime
from optparse import OptionGroup

from yelp_batch import batch_command_line_options
from yelp_batch import batch_configure
from yelp_batch import batch_context
from yelp_batch.for_each import Batch
from yelp_batch._db import BatchDBMixin
from yelp_lib.classutil import cached_property
from yelp_servlib import config_util


class FullRefreshRunner(Batch, BatchDBMixin):
    db_name = 'primary'
    notify_emails = ['bam+batch@yelp.com']
    is_readonly_batch = False
    ro_replica_name = 'batch_ro'
    rw_replica_name = 'batch_rw'

    @cached_property
    def total_row_count(self):
        query = 'SELECT COUNT(*) FROM {0}'.format(self.table_name)
        value = self.execute_sql(query, is_write_session=False).fetchone()
        return value[0] if value is not None else 1

    @batch_configure
    def configure(self):
        config_util.load_package_config(self.options.config_path, field='module_config')
        yelp_conn.initialize()

    @batch_context
    def _session_manager(self):
        self._wait_for_replication()
        with self.read_session() as self._read_session, self.write_session() as self._write_session:
            yield

    @batch_command_line_options
    def define_options(self, option_parser):
        opt_group = OptionGroup(option_parser, 'Full Refresh Runner Options')

        opt_group.add_option(
            '--table-name',
            dest='table_name',
            help='Name of table to be refreshed (default: %default).'
        )

        opt_group.add_option(
            '--batch-size',
            dest='batch_size',
            type='int',
            default=100,
            help='Number of rows to process between commits (default: %default).'
        )

        opt_group.add_option('--dry-run', action="store_true", dest='dry_run', default=False)

        opt_group.add_option(
            '--config-path',
            dest='config_path',
            default='/nail/home/psuben/pg/yelp-main/config/test_config.yaml',
            help='Config file path for FullRefreshRunner'
        )

        opt_group.add_option(
            '--no-start-up-replication-wait',
            dest='wait_for_replication_on_startup',
            action='store_false', default=True,
            help='On startup, do not wait for replication to catch up to the '
                 'time batch was started (default: %default).'
        )
        return opt_group

    @batch_configure
    def _init_global_state(self):
        if self.options.batch_size <= 0:
            raise ValueError("batch size should be greater than 0")

        self.table_name = self.options.table_name
        # TODO(psuben|2015-09-30): Decide the actual naming convention for blackhole table
        self.temp_table = 'temp_{0}'.format(self.table_name)
        self.processed_row_count = 0

    def _wait_for_replication(self):
        """Lets first wait for ro_conn replication to catch up with the
        batch start time.
        """
        if self.options.wait_for_replication_on_startup:
            self.log.info(
                "Waiting for ro_conn replication to catch up with start time "
                "(%s)" % datetime.fromtimestamp(self.starttime))
            with self.ro_conn() as ro_conn:
                self.wait_for_replication_until(self.starttime, ro_conn)

    def generate_new_query(self):
        """Generates query for an identical table structure with a Blackhole engine.
        """
        show_original_query = 'SHOW CREATE TABLE {0}'.format(self.table_name)
        original_query = self.execute_sql(show_original_query, is_write_session=False).fetchone()[1]
        max_replacements = 1
        new_query = original_query.replace(self.table_name, self.temp_table, max_replacements)
        # Substitute original engine with Blackhole engine
        new_query = re.sub('ENGINE=[^\s]*', 'ENGINE=BLACKHOLE', new_query)
        self.log.info("New blackhole table query: {query}".format(query=new_query))
        return new_query

    def initial_action(self):
        self.total_row_count
        query = self.generate_new_query()
        self.execute_sql(query, is_write_session=True)
        self._after_processing_rows()

    def final_action(self):
        query = 'DROP TABLE {0}'.format(self.temp_table)
        self.execute_sql(query, is_write_session=True)
        self.log.info("Dropped table: {table}".format(table=self.temp_table))

    def process_table(self, row_generator):
        remaining_row_count = self.total_row_count
        while remaining_row_count > 0:
            chunk_size = min(self.options.batch_size, remaining_row_count)
            for i in range(chunk_size):
                self.insert_rows_into_temp(row_generator)

            self.log.info(
                "Inserted {chunk_size} rows into {table}".format(
                    chunk_size=chunk_size,
                    table=self.temp_table
                )
            )
            self._after_processing_rows()
            remaining_row_count -= chunk_size
            self.processed_row_count += chunk_size

    def run(self):
        self.initial_action()
        self.log.info(
            "Total rows to be processed: {row_count}".format(
                row_count=self.total_row_count
            )
        )
        row_generator = self.get_row()
        self.process_table(row_generator)
        self.log_run_info()
        self.final_action()

    def insert_rows_into_temp(self, row_generator):
        # String manipulation to make sure strings are not mistaken for Columns
        row_values = ','.join(
            '\'{0}\''.format(str(column_values))
            for column_values in next(row_generator).values()
        )
        query = 'INSERT INTO {0} VALUES ({1})'.format(self.temp_table, row_values)
        self.execute_sql(query, is_write_session=True)
        self.log.info("Row inserted: {query}".format(query=query))

    def get_row(self):
        query = 'SELECT * FROM {0}'.format(self.table_name)
        result = self.execute_sql(query, is_write_session=False)
        for row in result:
            yield row

    def execute_sql(self, query, is_write_session):
        if is_write_session:
            if not self.options.dry_run:
                return self._write_session.execute(query)
            else:
                self.log.info("Dry run: Query: {0}".format(query))
        else:
            return self._read_session.execute(query)

    def _after_processing_rows(self):
        """Commits changes and makes sure replication catches up before moving on.
        """
        self._commit_changes()
        with self.rw_conn() as rw_conn:
            self.throttle_to_replication(rw_conn)

    def _commit_changes(self):
        self._read_session.rollback()
        if not self.options.dry_run:
            self._write_session.commit()
        else:
            self.log.info("Dry run: Writes would be committed here.")

    def log_run_info(self):
        elapsed_time = timedelta(seconds=(time.time() - self.starttime))
        self.log.info(
            "Processed {row_count} row(s) in {elapsed_time}".format(
                row_count=self.processed_row_count,
                elapsed_time=elapsed_time
            )
        )


if __name__ == '__main__':
    FullRefreshRunner().start()
