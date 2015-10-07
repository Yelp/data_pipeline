# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import re
import time
from datetime import datetime
from optparse import OptionGroup

import yelp_conn
from yelp_batch import batch_command_line_options
from yelp_batch import batch_configure
from yelp_batch import batch_context
from yelp_batch._db import BatchDBMixin
from yelp_batch.for_each import Batch
from yelp_lib.classutil import cached_property
from yelp_servlib import config_util


class FullRefreshRunner(Batch, BatchDBMixin):
    notify_emails = ['bam+batch@yelp.com']
    is_readonly_batch = False
    ro_replica_name = str('batch_ro')
    rw_replica_name = str('batch_rw')

    @batch_context
    def _session_manager(self):
        self._wait_for_replication()
        with self.read_session() as self._read_session, \
                self.write_session() as self._write_session:
            yield

    @batch_command_line_options
    def define_options(self, option_parser):
        opt_group = OptionGroup(option_parser, 'Full Refresh Runner Options')

        opt_group.add_option(
            '--cluster',
            dest='cluster',
            default='primary',
            help='Specifies table cluster (default: %default).'
        )
        opt_group.add_option(
            '--table-name',
            dest='table_name',
            help='Required: Name of table to be refreshed.'
        )
        opt_group.add_option(
            '--batch-size',
            dest='batch_size',
            type='int',
            default=100,
            help='Number of rows to process between commits '
                 '(default: %default).'
        )
        opt_group.add_option(
            '--primary',
            dest='primary',
            help='Required: Comma separated string of primary key column names'
        )
        opt_group.add_option(
            '--dry-run',
            action="store_true",
            dest='dry_run',
            default=False
        )
        opt_group.add_option(
            '--config-path',
            dest='config_path',
            help='Required: Config file path for FullRefreshRunner'
        )
        opt_group.add_option(
            '--where',
            dest='where_clause',
            default=True,
            help='Custom where clause to specify which rows to refresh '
                 'Note: This option takes everything that would come '
                 'after the WHERE in a sql statement.'
        )
        opt_group.add_option(
            '--no-start-up-replication-wait',
            dest='wait_for_replication_on_startup',
            action='store_false',
            default=True,
            help='On startup, do not wait for replication to catch up to the '
                 'time batch was started (default: %default).'
        )
        return opt_group

    @batch_configure
    def configure(self):
        config_util.load_package_config(
            self.options.config_path,
            field='module_config'
        )
        yelp_conn.initialize()

    @batch_configure
    def _init_global_state(self):
        if self.options.batch_size <= 0:
            raise ValueError("Batch size should be greater than 0")

        self.db_name = self.options.cluster
        self.table_name = self.options.table_name
        self.temp_table = '{table}_data_pipeline_refresh'.format(
            table=self.table_name
        )
        self.primary_key = self.options.primary
        self.processed_row_count = 0
        self.where_clause = self.options.where_clause

    @cached_property
    def total_row_count(self):
        query = 'SELECT COUNT(*) FROM {table_name} WHERE {clause}'.format(
            table_name=self.table_name,
            clause=self.where_clause
        )
        value = self.execute_sql(query, is_write_session=False).scalar()
        return value if value is not None else 0

    def _wait_for_replication(self):
        """Lets first wait for ro_conn replication to catch up with the
        batch start time.
        """
        if self.options.wait_for_replication_on_startup:
            self.log.info(
                "Waiting for ro_conn replication to catch up with start time "
                "{start_time}".format(
                    start_time=datetime.fromtimestamp(self.starttime)
                )
            )
            with self.ro_conn() as ro_conn:
                self.wait_for_replication_until(self.starttime, ro_conn)

    def execute_sql(self, query, is_write_session):
        if is_write_session:
            if not self.options.dry_run:
                return self._write_session.execute(query)
            else:
                self.log.info("Dry run: Query: {query}".format(query=query))
        else:
            return self._read_session.execute(query)

    def create_table_from_src_table(self):
        show_create_statement = 'SHOW CREATE TABLE {table_name}'.format(
            table_name=self.table_name
        )
        original_query = self.execute_sql(
            show_create_statement,
            is_write_session=False
        ).fetchone()[1]
        max_replacements = 1
        refresh_table_create_query = original_query.replace(
            self.table_name,
            self.temp_table,
            max_replacements
        )
        # Substitute original engine with Blackhole engine
        refresh_table_create_query = re.sub(
            'ENGINE=[^\s]*',
            'ENGINE=BLACKHOLE',
            refresh_table_create_query
        )
        self.log.info("New blackhole table query: {query}".format(
            query=refresh_table_create_query
        ))
        self.execute_sql(refresh_table_create_query, is_write_session=True)

    def _after_processing_rows(self):
        """Commits changes and makes sure replication catches up
        before moving on.
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

    def initial_action(self):
        self.create_table_from_src_table()
        self._after_processing_rows()

    def final_action(self):
        query = 'DROP TABLE IF EXISTS {temp_table}'.format(
            temp_table=self.temp_table
        )
        self.execute_sql(query, is_write_session=True)
        self.log.info("Dropped table: {table}".format(table=self.temp_table))

    def setup_transaction(self):
        self.execute_sql('BEGIN', is_write_session=True)
        self.execute_sql(
            'LOCK TABLES {table} WRITE, {temp} WRITE'.format(
                table=self.table_name,
                temp=self.temp_table
            ),
            is_write_session=True
        )

    def count_inserted(self, offset):
        query = (
            'SELECT COUNT(*) FROM (SELECT * FROM {origin} WHERE {clause} '
            'ORDER BY {pk} LIMIT {offset}, {batch_size}) AS T'
        ).format(
            origin=self.table_name,
            clause=self.where_clause,
            pk=self.primary_key,
            offset=offset,
            batch_size=self.options.batch_size
        )
        inserted_rows = self.execute_sql(query, is_write_session=False)
        return inserted_rows.scalar()

    def insert_batch(self, offset):
        insert_query = (
            'INSERT INTO {temp} SELECT * FROM {origin} WHERE {clause} '
            'ORDER BY {pk} LIMIT {offset}, {batch_size}'
        ).format(
            temp=self.temp_table,
            origin=self.table_name,
            clause=self.where_clause,
            pk=self.primary_key,
            offset=offset,
            batch_size=self.options.batch_size
        )
        self.execute_sql(insert_query, is_write_session=True)

    def process_table(self):
        self.log.info(
            "Total rows to be processed: {row_count}".format(
                row_count=self.total_row_count
            )
        )
        offset = 0
        count = self.options.batch_size
        while count >= self.options.batch_size:
            self.setup_transaction()
            count = self.count_inserted(offset)
            self.insert_batch(offset)
            self._after_processing_rows()
            offset += count
            self.processed_row_count += count

    def log_info(self):
        elapsed_time = time.time() - self.starttime
        self.log.info(
            "Processed {row_count} row(s) in {elapsed_time}".format(
                row_count=self.processed_row_count,
                elapsed_time=elapsed_time
            )
        )

    def run(self):
        try:
            self.initial_action()
            self.process_table()
            self.log_info()
        finally:
            self.final_action()


if __name__ == '__main__':
    FullRefreshRunner().start()
