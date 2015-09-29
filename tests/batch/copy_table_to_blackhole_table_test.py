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
        return "temp_" + table_name

    @pytest.fixture
    def fake_query(self):
        return 'SELECT * FROM faketable'

    @pytest.yield_fixture
    def refresh_batch(self, table_name):
        batch = FullRefreshRunner()
        batch.process_commandline_options(['--dry-run', '--table-name=' + table_name])
        batch._init_global_state()
        batch.setup_yelp_conn = mock.Mock()
        batch.wait_for_replication_until = mock.Mock()
        batch.throttle_to_replication = mock.Mock()
        yield batch

    @pytest.yield_fixture
    def sessions(self, refresh_batch):
        with mock.patch.object(refresh_batch, 'read_session', autospec=True) as mock_read, \
                mock.patch.object(refresh_batch, 'write_session', autospec=True) as mock_write, \
                refresh_batch.read_session() as refresh_batch._read_session, \
                refresh_batch.write_session() as refresh_batch._write_session:
            yield

    def test_initial_action(self, refresh_batch, table_name, temp_name):
        with mock.patch.object(FullRefreshRunner, '_after_processing_rows') as mock_process_rows, \
                mock.patch.object(
                    FullRefreshRunner,
                    'total_row_count',
                    new_callable=mock.PropertyMock
                ) as mock_row_count, \
                mock.patch.object(refresh_batch, 'execute_sql') as mock_write:
            refresh_batch.initial_action()
            mock_row_count.assert_called_once_with()
            mock_process_rows.assert_called_once_with()
            mock_write.assert_called_once_with(
                'CREATE TABLE IF NOT EXISTS ' + temp_name + ' LIKE ' + table_name,
                is_write_session=True
            )

    def test_final_action(self, refresh_batch, temp_name):
        with mock.patch.object(refresh_batch, 'execute_sql') as mock_write:
            refresh_batch.final_action()
            mock_write.assert_called_once_with('DROP TABLE ' + temp_name, is_write_session=True)

    def test_after_row_processing(self, refresh_batch, sessions):
        refresh_batch._commit_changes()
        refresh_batch._read_session.rollback.assert_called_once_with()
        refresh_batch._write_session.commit.assert_not_called()

    def test_execute_sql_read(self, refresh_batch, sessions, fake_query):
        refresh_batch.execute_sql(fake_query, is_write_session=False)
        refresh_batch._read_session.execute.assert_called_once_with(fake_query)
        refresh_batch._write_session.execute.assert_not_called()

    def test_execute_sql_write(self, refresh_batch, sessions, fake_query):
        refresh_batch.execute_sql(fake_query, is_write_session=True)
        refresh_batch._read_session.execute.assert_not_called()
        refresh_batch._write_session.execute.assert_not_called()

    def test_get_row(self, refresh_batch, table_name):
        with mock.patch.object(refresh_batch, 'execute_sql', return_value=[]) as mock_write:
            assert tuple(refresh_batch.get_row()) == ()
            mock_write.assert_called_once_with('SELECT * FROM ' + table_name, is_write_session=False)
