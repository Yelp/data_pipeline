# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline.batch.copy_table_to_blackhole_table import FullRefreshRunner


class TestFullRefreshRunner(object):

    @pytest.fixture
    def table_name(self):
        return "test_db"

    @pytest.fixture
    def temp_name(self, table_name):
        return "{name}_data_pipeline_refresh".format(name=table_name)

    @pytest.fixture
    def show_table_query(self, table_name):
        return 'SHOW CREATE TABLE {0}'.format(table_name)

    @pytest.fixture
    def fake_query(self):
        return 'SELECT * FROM faketable'

    @pytest.fixture
    def fake_original_table(self):
        return (
            'CREATE TABLE test_db('
            'PersonID int,'
            'LastName varchar(255),'
            'FirstName varchar(255),'
            'Address varchar(255),'
            'City varchar(255))'
            'ENGINE=InnoDB'
        )

    @pytest.fixture
    def fake_new_table(self):
        return (
            'CREATE TABLE test_db_data_pipeline_refresh('
            'PersonID int,'
            'LastName varchar(255),'
            'FirstName varchar(255),'
            'Address varchar(255),'
            'City varchar(255))'
            'ENGINE=BLACKHOLE'
        )

    @pytest.yield_fixture
    def refresh_batch(self, table_name):
        batch = FullRefreshRunner()
        batch.process_commandline_options([
            '--dry-run',
            '--table-name={0}'.format(table_name),
            '--primary=id'
        ])
        batch._init_global_state()
        yield batch

    @pytest.yield_fixture
    def _read(self, refresh_batch):
        with mock.patch.object(
                refresh_batch,
                'read_session',
                autospec=True
        ) as mock_read_session:
            yield mock_read_session

    @pytest.yield_fixture
    def _write(self, refresh_batch):
        with mock.patch.object(
                refresh_batch,
                'write_session',
                autospec=True
        ) as mock_write_session:
            yield mock_write_session

    @pytest.yield_fixture
    def _read_session(self, refresh_batch):
        with refresh_batch.read_session() as refresh_batch._read_session:
            yield

    @pytest.yield_fixture
    def _write_session(self, refresh_batch):
        with refresh_batch.write_session() as refresh_batch._write_session:
            yield

    @pytest.yield_fixture
    def sessions(
        self,
        refresh_batch,
        _read,
        _write,
        _read_session,
        _write_session
    ):
        yield

    @pytest.yield_fixture
    def mock_process_rows(self):
        with mock.patch.object(
                FullRefreshRunner,
                '_after_processing_rows'
        ) as mock_process_rows:
            yield mock_process_rows

    @pytest.yield_fixture
    def mock_row_count(self):
        with mock.patch.object(
            FullRefreshRunner,
            'total_row_count',
            new_callable=mock.PropertyMock
        ) as mock_row_count:
            yield mock_row_count

    @pytest.yield_fixture
    def mock_execute(self):
        with mock.patch.object(
            FullRefreshRunner,
            'execute_sql'
        ) as mock_execute:
            yield mock_execute

    @pytest.yield_fixture
    def mock_create_table_src(self):
        with mock.patch.object(
            FullRefreshRunner,
            'create_table_from_src_table'
        ) as mock_create:
            yield mock_create

    def test_initial_action(
        self,
        refresh_batch,
        mock_process_rows,
        mock_create_table_src
    ):
        refresh_batch.initial_action()
        mock_create_table_src.assert_called_once_with()
        mock_process_rows.assert_called_once_with()

    def test_final_action(self, refresh_batch, temp_name, mock_execute):
        refresh_batch.final_action()
        mock_execute.assert_called_once_with(
            'DROP TABLE IF EXISTS {0}'.format(temp_name),
            is_write_session=True
        )

    def test_after_row_processing(self, refresh_batch, sessions):
        refresh_batch._commit_changes()
        refresh_batch._read_session.rollback.assert_called_once_with()
        refresh_batch._write_session.commit.assert_not_called()

    def test_create_table_from_src_table(
        self,
        refresh_batch,
        fake_original_table,
        fake_new_table,
        show_table_query
    ):
        with mock.patch.object(
                refresh_batch,
                'execute_sql',
                autospec=True
        ) as mock_execute:
            mock_execute.return_value.fetchone.return_value = [
                'test_db',
                fake_original_table
            ]
            refresh_batch.create_table_from_src_table()
            calls = [
                mock.call(show_table_query, is_write_session=False),
                mock.call(fake_new_table, is_write_session=True)
            ]
            mock_execute.assert_has_calls(calls, any_order=True)

    def test_execute_sql_read(self, refresh_batch, sessions, fake_query):
        refresh_batch.execute_sql(fake_query, is_write_session=False)
        refresh_batch._read_session.execute.assert_called_once_with(fake_query)
        refresh_batch._write_session.execute.assert_not_called()

    def test_execute_sql_write(self, refresh_batch, sessions, fake_query):
        refresh_batch.execute_sql(fake_query, is_write_session=True)
        refresh_batch._read_session.execute.assert_not_called()
        refresh_batch._write_session.execute.assert_not_called()

    def test_insert_batch(
        self,
        refresh_batch,
        mock_execute,
        table_name,
        temp_name
    ):
        offset = 0
        refresh_batch.insert_batch(offset)
        query = ('INSERT INTO {0} SELECT * FROM {1} '
                 'ORDER BY id LIMIT {2}, {3}').format(
            temp_name,
            table_name,
            offset,
            refresh_batch.options.batch_size
        )
        mock_execute.assert_called_once_with(query, is_write_session=True)

    def test_process_table(
        self,
        refresh_batch,
        mock_row_count,
        mock_process_rows
    ):
        with mock.patch.object(
            refresh_batch,
            'insert_batch'
        ) as mock_insert, mock.patch.object(
            refresh_batch,
            'count_inserted'
        ) as mock_rows, mock.patch.object(
            refresh_batch,
            'options',
            autospec=True
        ) as mock_options:
            mock_rows.side_effect = [10, 10, 5]
            mock_options.batch_size = 10
            mock_row_count.return_value = 25
            refresh_batch.process_table()
            calls = [mock.call(0), mock.call(10), mock.call(20)]
            mock_insert.assert_has_calls(calls)
