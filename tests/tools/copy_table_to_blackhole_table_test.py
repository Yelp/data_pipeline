# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import mock
import pytest

from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
from data_pipeline.tools.copy_table_to_blackhole_table import FullRefreshRunner
from data_pipeline.tools.copy_table_to_blackhole_table import TopologyFile


# TODO(justinc|DATAPIPE-710): These tests are a little overly complicated and
# should be refactored.  In particular - it would be nice to simplify this so
# that only public methods are tested.  Mocking just enough to see what SQL
# queries are executed with fixed data should be enough.
class TestFullRefreshRunner(object):

    @pytest.fixture
    def base_path(self):
        return "data_pipeline.tools.copy_table_to_blackhole_table"

    @pytest.fixture
    def topology_path(self):
        return "/nail/srv/configs/topology.yaml"

    @pytest.fixture
    def cluster(self):
        return "test_cluster"

    @pytest.fixture
    def database_name(self):
        return "yelp"

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
            'CREATE TABLE IF NOT EXISTS test_db_data_pipeline_refresh('
            'PersonID int,'
            'LastName varchar(255),'
            'FirstName varchar(255),'
            'Address varchar(255),'
            'City varchar(255))'
            'ENGINE=BLACKHOLE'
        )

    @pytest.fixture
    def refresh_params(self, cluster, table_name, database_name):
        return {
            'refresh_id': 1,
            'cluster': cluster,
            'database': database_name,
            'config_path': 'test_config.yaml',
            'table_name': table_name,
            'offset': 0,
            'batch_size': 200,
            'primary': 'id',
            'where_clause': None,
            'dry_run': True,
        }

    @pytest.yield_fixture
    def mock_get_schematizer(self, base_path):
        with mock.patch(base_path + '.get_schematizer'):
            yield

    @pytest.yield_fixture
    def mock_load_config(self, base_path):
        with mock.patch(base_path + '.load_default_config'):
            yield

    @pytest.yield_fixture
    def refresh_batch(
        self,
        cluster,
        table_name,
        topology_path,
        mock_load_config,
        database_name
    ):
        batch = FullRefreshRunner()
        batch.process_commandline_options([
            '--dry-run',
            '--table-name={0}'.format(table_name),
            '--primary=id',
            '--cluster={0}'.format(cluster),
            '--topology-path={0}'.format(topology_path),
            '--database={0}'.format(database_name)
        ])
        batch._init_global_state()
        yield batch

    @pytest.yield_fixture
    def refresh_batch_custom_where(self, table_name, mock_load_config, database_name):
        batch = FullRefreshRunner()
        batch.process_commandline_options([
            '--dry-run',
            '--table-name={0}'.format(table_name),
            '--primary=id',
            '--where={0}'.format("country='CA'"),
            '--database={0}'.format(database_name)
        ])
        batch._init_global_state()
        yield batch

    @pytest.yield_fixture
    def managed_refresh_batch(
        self,
        mock_get_schematizer,
        mock_load_config,
        refresh_params
    ):
        # Initialize the batch the same way the refresh manager would.
        batch = FullRefreshRunner(**refresh_params)
        with mock.patch.object(
            batch,
            'get_connection_set_from_cluster'
        ):
            batch.setup_connections()
            yield batch

    @pytest.yield_fixture
    def managed_refresh_batch_custom_where(
        self,
        mock_load_config,
        refresh_params
    ):
        refresh_params['where_clause'] = "country='CA'"
        batch = FullRefreshRunner(**refresh_params)
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
    def read_session(self, refresh_batch, _read):
        with refresh_batch.read_session() as read_session:
            yield read_session

    @pytest.yield_fixture
    def write_session(self, refresh_batch, _write):
        with refresh_batch.write_session() as write_session:
            yield write_session

    @pytest.yield_fixture
    def _managed_write(self, managed_refresh_batch):
        with mock.patch.object(
            managed_refresh_batch,
            'write_session',
            autospec=True
        ) as mock_write_session:
            yield mock_write_session

    @pytest.yield_fixture
    def managed_write_session(self, managed_refresh_batch, _managed_write):
        with managed_refresh_batch.write_session() as write_session:
            yield write_session

    @pytest.yield_fixture
    def _rw_conn(self, refresh_batch):
        with mock.patch.object(
            refresh_batch,
            'rw_conn'
        ) as mock_rw_conn:
            yield mock_rw_conn

    @pytest.yield_fixture
    def rw_conn(self, _rw_conn):
        with _rw_conn() as conn:
            yield conn

    @pytest.yield_fixture
    def _ro_conn(self, refresh_batch):
        with mock.patch.object(
            refresh_batch,
            'ro_conn'
        ) as mock_ro_conn:
            yield mock_ro_conn

    @pytest.yield_fixture
    def ro_conn(self, _ro_conn):
        with _ro_conn() as conn:
            yield conn

    @pytest.yield_fixture
    def sessions(
        self,
        refresh_batch,
        _read,
        _write,
        read_session,
        write_session
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
            '_execute_query'
        ) as mock_execute:
            yield mock_execute

    @pytest.yield_fixture
    def mock_create_table_src(self):
        with mock.patch.object(
            FullRefreshRunner,
            'create_table_from_src_table'
        ) as mock_create:
            yield mock_create

    def test_setup_connections(
        self,
        base_path,
        refresh_batch,
        cluster
    ):
        with mock.patch(
            base_path + '.TransactionManager'
        ) as mock_manager, mock.patch.object(
            refresh_batch,
            'get_connection_set_from_cluster'
        ) as mock_get_conn:
            refresh_batch.setup_connections()
            mock_manager.assert_called_once_with(
                cluster_name=cluster,
                ro_replica_name=cluster,
                rw_replica_name=cluster,
                connection_set_getter=mock_get_conn
            )

    def test_initial_action_with_db(
        self,
        database_name,
        refresh_batch,
        mock_execute,
        mock_process_rows,
        mock_create_table_src,
        sessions,
        write_session
    ):
        with mock.patch.object(
            refresh_batch,
            '_wait_for_replication'
        ) as wait_for_replication_mock:
            refresh_batch.initial_action()
        assert wait_for_replication_mock.call_count == 1
        mock_execute.assert_called_once_with(
            write_session,
            "USE {0}".format(database_name),
        )
        mock_create_table_src.assert_called_once_with(write_session)
        assert write_session.rollback.call_count == 1

    def test_initial_action_managed_refresh(
        self,
        database_name,
        managed_refresh_batch,
        mock_execute,
        mock_process_rows,
        mock_create_table_src,
        sessions,
        managed_write_session
    ):
        with mock.patch.object(
            managed_refresh_batch,
            '_wait_for_replication'
        ) as wait_for_replication_mock:
            managed_refresh_batch.initial_action()
        assert wait_for_replication_mock.call_count == 1
        update_refresh = managed_refresh_batch.schematizer.update_refresh
        update_refresh.assert_called_once_with(
            1,
            RefreshStatus.IN_PROGRESS,
            0
        )
        mock_execute.assert_called_once_with(
            managed_write_session,
            "USE {0}".format(database_name),
        )
        mock_create_table_src.assert_called_once_with(managed_write_session)
        assert managed_write_session.rollback.call_count == 1

    def test_final_action(
        self,
        refresh_batch,
        temp_name,
        write_session,
        mock_execute,
        database_name
    ):
        refresh_batch.final_action()
        calls = [
            mock.call(write_session, 'USE {0}'.format(database_name)),
            mock.call(
                write_session,
                'DROP TABLE IF EXISTS {0}'.format(temp_name),
            ),
        ]
        mock_execute.assert_has_calls(calls)

    def test_after_row_processing(self, refresh_batch, write_session, rw_conn):
        with mock.patch.object(
            refresh_batch,
            'throttle_to_replication'
        ) as throttle_mock, mock.patch.object(
            refresh_batch,
            '_wait_for_throughput',
            return_value=None
        ) as mock_wait:
            # count can be anything since self.avg_throughput_cap is set to None
            refresh_batch._after_processing_rows(write_session, count=0)

        assert write_session.rollback.call_count == 1
        write_session.execute.assert_called_once_with('UNLOCK TABLES')
        assert write_session.commit.call_count == 1
        throttle_mock.assert_called_once_with(rw_conn)
        assert mock_wait.call_count == 1
        assert refresh_batch.avg_rows_per_second_cap == refresh_batch.DEFAULT_AVG_ROWS_PER_SECOND_CAP

    def test_build_select(
        self,
        refresh_batch,
        refresh_batch_custom_where,
        table_name
    ):
        offset = 0
        batch_size = refresh_batch_custom_where.options.batch_size
        expected_where_query = (
            'SELECT * FROM {origin} WHERE {clause} ORDER BY id '
            'LIMIT {offset}, {batch_size}'
        ).format(
            origin=table_name,
            clause="country='CA'",
            offset=offset,
            batch_size=batch_size
        )
        where_query = refresh_batch_custom_where.build_select(
            '*',
            'id',
            offset,
            batch_size
        )
        expected_count_query = 'SELECT COUNT(*) FROM {origin}'.format(
            origin=table_name
        )
        count_query = refresh_batch.build_select('COUNT(*)')
        assert expected_where_query == where_query
        assert expected_count_query == count_query

    def test_create_table_from_src_table(
        self,
        refresh_batch,
        fake_original_table,
        fake_new_table,
        show_table_query,
        write_session
    ):
        with mock.patch.object(
            refresh_batch,
            '_execute_query',
            autospec=True
        ) as mock_execute:
            mock_execute.return_value.fetchone.return_value = [
                'test_db',
                fake_original_table
            ]
            refresh_batch.create_table_from_src_table(write_session)
            calls = [
                mock.call(write_session, show_table_query),
                mock.call(write_session, fake_new_table)
            ]
            mock_execute.assert_has_calls(calls, any_order=True)

    def test_execute_query(self, refresh_batch, write_session, fake_query):
        refresh_batch._execute_query(write_session, fake_query)
        write_session.execute.assert_called_once_with(fake_query)

    def insert_batch_test_helper(
        self,
        batch,
        session,
        temp_name,
        table_name,
        mock_execute,
        clause
    ):
        min_pk = 1
        max_pk = 2
        batch.insert_batch(session, min_pk, max_pk)

        # The queries below are formatted this way to match the whitespace of
        # the original query that was being called for the purposes of
        # assertion
        if clause is not None:
            query = """INSERT INTO {0}
        SELECT * FROM {1}
        WHERE id>{2} AND id<={3}
         AND {4}""".format(
                temp_name,
                table_name,
                min_pk,
                max_pk,
                clause,
                batch.options.batch_size
            )
        else:
            query = """INSERT INTO {0}
        SELECT * FROM {1}
        WHERE id>{2} AND id<={3}
        """.format(
                temp_name,
                table_name,
                min_pk,
                max_pk,
            )
        mock_execute.assert_called_once_with(session, query)

    def test_insert_batch_default_where(
        self,
        refresh_batch,
        mock_execute,
        table_name,
        temp_name,
        write_session
    ):
        clause = None
        self.insert_batch_test_helper(
            refresh_batch,
            write_session,
            temp_name,
            table_name,
            mock_execute,
            clause
        )

    def test_insert_batch_custom_where(
        self,
        refresh_batch_custom_where,
        temp_name,
        table_name,
        mock_execute,
        write_session,
    ):
        clause = "country='CA'"
        self.insert_batch_test_helper(
            refresh_batch_custom_where,
            write_session,
            temp_name,
            table_name,
            mock_execute,
            clause
        )

    @pytest.fixture(params=[
        {
            'min_ret_val': 1,
            'max_ret_val': 31,
            'row_side_eff': [10, 10, 10, 1],
            'row_count': 31,
            'calls': [(0, 10), (10, 20), (20, 30), (30, 40)]
        },
        {
            'min_ret_val': 1,
            'max_ret_val': 30,
            'row_side_eff': [10, 10, 10, 0],
            'row_count': 30,
            'calls': [(0, 10), (10, 20), (20, 30), (30, 40)]
        },
        {
            'min_ret_val': 1,
            'max_ret_val': 29,
            'row_side_eff': [10, 10, 9],
            'row_count': 29,
            'calls': [(0, 10), (10, 20), (20, 30)]
        }
        ]
    )
    def inputs(self, request):
        return request.param

    def test_process_table(
        self,
        refresh_batch,
        mock_row_count,
        mock_process_rows,
        sessions,
        write_session,
        read_session,
        inputs
    ):
        with mock.patch.object(
            refresh_batch,
            'insert_batch'
        ) as mock_insert, mock.patch.object(
            refresh_batch,
            'count_inserted'
        ) as mock_rows, mock.patch.object(
            refresh_batch,
            '_get_min_primary_key'
        ) as mock_min_pk, mock.patch.object(
            refresh_batch,
            '_get_max_primary_key'
        ) as mock_max_pk, mock.patch.object(
            refresh_batch,
            'batch_size',
            10
        ):
            mock_min_pk.return_value = inputs['min_ret_val']
            mock_max_pk.return_value = inputs['max_ret_val']
            mock_rows.side_effect = inputs['row_side_eff']
            mock_row_count.return_value = inputs['row_count']
            refresh_batch.process_table()
            call_inputs = inputs['calls']
            calls = []
            for x, y in call_inputs:
                calls.append(
                    mock.call(write_session, x, y)
                )
            mock_insert.assert_has_calls(calls)
            
    def test_process_table_managed_refresh(
        self,
        managed_refresh_batch,
        mock_row_count,
        mock_process_rows,
        sessions,
        managed_write_session
    ):
        with mock.patch.object(
            managed_refresh_batch,
            'insert_batch'
        ) as mock_insert, mock.patch.object(
            managed_refresh_batch,
            'count_inserted'
        ) as mock_rows, mock.patch.object(
            managed_refresh_batch,
            '_get_min_primary_key'
        ) as mock_min_pk, mock.patch.object(
            managed_refresh_batch,
            '_get_max_primary_key'
        ) as mock_max_pk, mock.patch.object(
            managed_refresh_batch,
            'batch_size',
            10
        ):
            mock_min_pk.return_value = 1
            mock_max_pk.return_value = 25
            mock_rows.side_effect = [10, 10, 5]
            mock_row_count.return_value = 25
            managed_refresh_batch.process_table()
            calls = [
                mock.call(managed_write_session, 0, 10),
                mock.call(managed_write_session, 10, 20),
                mock.call(managed_write_session, 20, 30)
            ]
            mock_insert.assert_has_calls(calls)
            managed_refresh_batch.schematizer.update_refresh.assert_called_once_with(
                refresh_id=1,
                status=RefreshStatus.SUCCESS,
                offset=0
            )

    def test_get_connection_set_from_cluster(
        self,
        refresh_batch,
        base_path,
        database_name,
        topology_path
    ):
        mock_topology = mock.Mock()
        mock_conn_defs = mock.Mock()
        mock_conn_config = mock.Mock()
        with mock.patch.object(
            TopologyFile,
            'new_from_file',
            return_value=mock_topology
        ) as mock_tf, mock.patch.object(
            refresh_batch,
            '_get_conn_defs',
            return_value=mock_conn_defs
        ) as mock_get_defs, mock.patch(
            base_path + '.ConnectionSetConfig',
            return_value=mock_conn_config
        ) as mock_init_config, mock.patch(
            base_path + '.ConnectionSet'
        ) as mock_conn:
            refresh_batch.get_connection_set_from_cluster(database_name)
            mock_tf.assert_called_once_with(topology_path)
            mock_get_defs.assert_called_once_with(
                mock_topology,
                database_name
            )
            mock_init_config.assert_called_once_with(
                database_name,
                mock_conn_defs,
                read_only=False
            )
            mock_conn.from_config.assert_called_once_with(mock_conn_config)

    def test_throughput_wait(
        self,
        refresh_batch
    ):
        with mock.patch.object(
            refresh_batch,
            'avg_rows_per_second_cap',
            1000
        ), mock.patch.object(
            refresh_batch,
            'process_row_start_time',
            0.0
        ), mock.patch.object(
            time,
            'time',
            return_value=0.1  # Simulating that it took 100 milliseconds to run the actual row processing
        ), mock.patch.object(
            time,
            'sleep',
            return_value=None
        ) as mock_sleep:
            refresh_batch._wait_for_throughput(count=100)
            mock_sleep.assert_called_with(0.0)
            refresh_batch._wait_for_throughput(count=1)
            mock_sleep.assert_called_with(0.0)
            refresh_batch._wait_for_throughput(count=1000)
            mock_sleep.assert_called_with(0.9)
            refresh_batch._wait_for_throughput(count=10000)
            mock_sleep.assert_called_with(9.9)
            refresh_batch._wait_for_throughput(count=500)
            mock_sleep.assert_called_with(0.4)
