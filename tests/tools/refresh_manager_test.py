# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import signal
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import mock
import pytest
import simplejson

from data_pipeline.schematizer_clientlib.models.refresh import _Refresh
from data_pipeline.schematizer_clientlib.models.refresh import Priority
from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.refresh_manager import FullRefreshManager
from data_pipeline.tools.refresh_manager import PriorityRefreshQueue

DEFAULT_CAP = 50


@pytest.mark.usefixures('containers')
class TestFullRefreshManager(object):

    def _create_schema(self, namespace, source):
        return simplejson.dumps(
            {
                "type": "record",
                "namespace": namespace,
                "name": source,
                "doc": "test",
                "fields": [
                    {"type": "int", "name": "field", "doc": "test_field", "default": 1}
                ]
            })

    def _create_job_from_refresh(
        self,
        refresh,
        throughput=0,
        last_throughput=None,
        status=None,
        pid=None
    ):
        job = {
            'id': refresh.refresh_id,
            'pid': pid,
            'throughput': throughput,
            'cap': refresh.avg_rows_per_second_cap or DEFAULT_CAP,
            'source': refresh.source_name,
            'priority': refresh.priority,
            'status': status or refresh.status
        }
        if last_throughput is not None:
            job['last_throughput'] = last_throughput
        return job

    @property
    def config_path(self):
        return '/nail/srv/configs/data_pipeline_tools.yaml'

    @property
    def source_owner_email(self):
        return "bam+test+introspection@yelp.com"

    @property
    def pid(self):
        return 42

    @pytest.fixture(scope='class')
    def schematizer(self, containers):
        return get_schematizer()

    @pytest.fixture
    def fake_worker(self):
        return namedtuple('Worker', ['pid'])(self.pid)

    @pytest.fixture
    def fake_cluster(self):
        return "refresh_primary"

    @pytest.fixture
    def fake_database(self):
        return "yelp{}".format(uuid4())

    @pytest.fixture
    def fake_namespace(self, fake_cluster, fake_database):
        return "{}.{}".format(fake_cluster, fake_database)

    @pytest.fixture
    def fake_source_name(self):
        return 'manager_source_{}'.format(uuid4())

    @pytest.fixture
    def fake_source_two_name(self):
        return 'manager_source_two_{}'.format(uuid4())

    @pytest.fixture
    def fake_topic(self, schematizer, fake_source_name, fake_namespace):
        schema = schematizer.register_schema(
            namespace=fake_namespace,
            source=fake_source_name,
            schema_str=self._create_schema(fake_namespace, fake_source_name),
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=None
        )
        return schema.topic

    @pytest.fixture(autouse=True)
    def fake_source(self, fake_topic):
        return fake_topic.source

    @pytest.fixture
    def fake_topic_two(self, schematizer, fake_source_two_name, fake_namespace):
        schema = schematizer.register_schema(
            namespace=fake_namespace,
            source=fake_source_two_name,
            schema_str=self._create_schema(fake_namespace, fake_source_two_name),
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=None
        )
        return schema.topic

    @pytest.fixture(autouse=True)
    def fake_source_two(self, fake_topic_two):
        return fake_topic_two.source

    @pytest.fixture
    def fake_created_at(self):
        return datetime(2015, 1, 1, 17, 0, 0)

    @pytest.fixture
    def fake_updated_at(self):
        return datetime(2015, 1, 1, 17, 0, 1)

    @pytest.fixture
    def refresh_params(
        self,
        fake_source_name,
        fake_namespace,
        fake_created_at,
        fake_updated_at
    ):
        return {
            'source_name': fake_source_name,
            'namespace_name': fake_namespace,
            'offset': 0,
            'batch_size': 200,
            'filter_condition': None,
            'created_at': fake_created_at,
            'updated_at': fake_updated_at
        }

    @pytest.fixture
    def refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 1
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = Priority.MEDIUM.value
        return _Refresh(**refresh_params).to_result()

    @pytest.fixture
    def paused_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 2
        refresh_params['status'] = 'PAUSED'
        refresh_params['priority'] = Priority.MEDIUM.value
        return _Refresh(**refresh_params).to_result()

    @pytest.fixture
    def high_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 3
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = Priority.HIGH.value
        return _Refresh(**refresh_params).to_result()

    @pytest.fixture
    def complete_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 4
        refresh_params['status'] = 'SUCCESS'
        refresh_params['priority'] = Priority.MEDIUM.value
        return _Refresh(**refresh_params).to_result()

    @pytest.fixture
    def refresh_result_two(self, refresh_params, fake_source_two_name):
        refresh_params['source_name'] = fake_source_two_name
        refresh_params['refresh_id'] = 5
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = Priority.MEDIUM.value
        return _Refresh(**refresh_params).to_result()

    @pytest.fixture
    def later_created_refresh_result(self, refresh_params, fake_created_at):
        refresh_params['refresh_id'] = 6
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = Priority.MEDIUM.value
        refresh_params['created_at'] = fake_created_at + timedelta(days=1)
        return _Refresh(**refresh_params).to_result()

    @pytest.fixture
    def real_refresh(self, schematizer, fake_source):
        return schematizer.create_refresh(
            source_id=fake_source.source_id,
            offset=0,
            batch_size=500,
            priority=Priority.MEDIUM.value,
            filter_condition=None,
            avg_rows_per_second_cap=(DEFAULT_CAP * 2)
            # if cap is too high we should still only use 50
        )

    @pytest.fixture
    def high_priority_real_refresh(self, schematizer, fake_source):
        return schematizer.create_refresh(
            source_id=fake_source.source_id,
            offset=0,
            batch_size=500,
            priority=Priority.HIGH.value,
            filter_condition=None,
            avg_rows_per_second_cap=None
        )

    @pytest.fixture
    def real_refresh_source_two(self, schematizer, fake_source_two):
        return schematizer.create_refresh(
            source_id=fake_source_two.source_id,
            offset=0,
            batch_size=500,
            priority=Priority.MEDIUM.value,
            filter_condition="country='CA' AND city='Waterloo'",
            avg_rows_per_second_cap=None
        )

    @pytest.fixture
    def refresh_manager(self, fake_namespace):
        refresh_manager = FullRefreshManager()
        refresh_manager.options = namedtuple(
            'Options',
            ['namespace', 'config_path', 'dry_run', 'verbose',
             'per_source_throughput_cap', 'total_throughput_cap']
        )(
            fake_namespace,
            self.config_path,
            True,
            0,
            DEFAULT_CAP,
            1000
        )
        refresh_manager._init_global_state()
        return refresh_manager

    @pytest.fixture
    def priority_refresh_queue(self):
        return PriorityRefreshQueue()

    @pytest.yield_fixture
    def mock_popen(self, fake_worker):
        with mock.patch(
            'data_pipeline.tools.refresh_manager.subprocess.Popen'
        ) as mock_popen:
            mock_popen.return_value = fake_worker
            yield mock_popen

    @pytest.yield_fixture
    def mock_kill(self):
        with mock.patch(
            'data_pipeline.tools.refresh_manager.os.kill'
        ) as mock_kill:
            yield mock_kill

    def test_refresh_queue_single_job(
        self,
        priority_refresh_queue,
        fake_source_name,
        refresh_result
    ):
        assert priority_refresh_queue.peek() == {}
        priority_refresh_queue.update([refresh_result])
        assert priority_refresh_queue.peek() == {fake_source_name: refresh_result}
        assert priority_refresh_queue.pop(fake_source_name) == refresh_result
        assert priority_refresh_queue.peek() == {}
        assert priority_refresh_queue.refresh_ref == {}
        assert priority_refresh_queue.source_to_refresh_queue == {}

    def test_refresh_queue_priority_and_paused_sort(
        self,
        priority_refresh_queue,
        fake_source_name,
        refresh_result,
        paused_refresh_result,
        high_refresh_result,
        later_created_refresh_result
    ):
        priority_refresh_queue.update(
            [paused_refresh_result, refresh_result,
             later_created_refresh_result, high_refresh_result]
        )
        assert priority_refresh_queue.peek() == {fake_source_name: high_refresh_result}
        assert priority_refresh_queue.pop(fake_source_name) == high_refresh_result
        assert priority_refresh_queue.peek() == {fake_source_name: paused_refresh_result}
        assert priority_refresh_queue.pop(fake_source_name) == paused_refresh_result
        assert priority_refresh_queue.peek() == {fake_source_name: refresh_result}
        assert priority_refresh_queue.pop(fake_source_name) == refresh_result
        assert priority_refresh_queue.peek() == {fake_source_name: later_created_refresh_result}

    def test_refresh_queue_multiple_sources(
        self,
        priority_refresh_queue,
        fake_source_name,
        fake_source_two_name,
        refresh_result,
        refresh_result_two
    ):
        priority_refresh_queue.update([refresh_result, refresh_result_two])
        refresh_set = priority_refresh_queue.peek()
        assert refresh_set[fake_source_name] == refresh_result
        assert refresh_set[fake_source_two_name] == refresh_result_two
        assert priority_refresh_queue.pop(fake_source_name) == refresh_result
        assert priority_refresh_queue.peek() == {fake_source_two_name: refresh_result_two}

    def test_refresh_runner_path(
        self,
        refresh_manager
    ):
        assert 'data_pipeline/tools/copy_table_to_blackhole_table.py' in \
            refresh_manager.refresh_runner_path

    def test_get_most_recent_topic_name(
        self,
        refresh_manager,
        fake_topic,
        fake_source_name,
        fake_namespace
    ):
        assert fake_topic.name == refresh_manager.get_most_recent_topic_name(
            fake_source_name, fake_namespace
        )

    def test_job_construction(
        self,
        refresh_manager,
        real_refresh,
        high_priority_real_refresh,
        fake_source
    ):

        expected_job = self._create_job_from_refresh(
            real_refresh
        )
        expected_high_priority_job = self._create_job_from_refresh(
            high_priority_real_refresh
        )
        refresh_manager.construct_running_refreshes(
            fake_source.name,
            real_refresh
        )
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_job]
        }
        refresh_manager.construct_running_refreshes(
            fake_source.name,
            high_priority_real_refresh
        )
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_high_priority_job, expected_job]
        }

    def test_step(
        self,
        refresh_manager,
        schematizer,
        real_refresh,
        high_priority_real_refresh,
        fake_source,
        fake_cluster,
        fake_database,
        mock_popen,
        mock_kill
    ):
        expected_job = self._create_job_from_refresh(
            real_refresh,
            throughput=DEFAULT_CAP,
            last_throughput=0,
            pid=self.pid
        )
        expected_high_priority_job = self._create_job_from_refresh(
            high_priority_real_refresh,
            throughput=DEFAULT_CAP,
            last_throughput=0,
            pid=self.pid
        )
        refresh_manager.step()
        mock_popen.assert_called_once_with(
            # testing refresh_runner_path is a seperate test
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(DEFAULT_CAP),
             '--batch-size={}'.format(high_priority_real_refresh.batch_size),
             '--refresh-id={}'.format(high_priority_real_refresh.refresh_id),
             '--dry-run', '--primary=id'
             ]
        )
        assert mock_kill.call_count == 0
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_high_priority_job]
        }
        assert refresh_manager.refresh_queue.peek() == {
            fake_source.name: real_refresh
        }
        high_priority_real_refresh = schematizer.update_refresh(
            high_priority_real_refresh.refresh_id,
            RefreshStatus.IN_PROGRESS,
            0
        )
        expected_high_priority_job['last_throughput'] = DEFAULT_CAP
        expected_high_priority_job['status'] = RefreshStatus.IN_PROGRESS
        refresh_manager.step()
        assert mock_kill.call_count == 0
        assert mock_popen.call_count == 1
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_high_priority_job]
        }
        assert refresh_manager.refresh_queue.peek() == {
            fake_source.name: real_refresh
        }
        high_priority_real_refresh = schematizer.update_refresh(
            high_priority_real_refresh.refresh_id,
            RefreshStatus.SUCCESS,
            0
        )
        refresh_manager.step()
        mock_popen.assert_called_with(
            # testing refresh_runner_path is a seperate test
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(DEFAULT_CAP),
             '--batch-size={}'.format(real_refresh.batch_size),
             '--refresh-id={}'.format(real_refresh.refresh_id),
             '--dry-run', '--primary=id'
             ]
        )
        assert mock_kill.call_count == 0
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_job]
        }
        assert refresh_manager.refresh_queue.peek() == {}
        schematizer.update_refresh(
            real_refresh.refresh_id,
            RefreshStatus.SUCCESS,
            0
        )
        refresh_manager.step()
        assert mock_popen.call_count == 2
        assert refresh_manager.active_refreshes == {}
        assert refresh_manager.refresh_queue.peek() == {}

    def test_multisource_step_low_cap(
        self,
        refresh_manager,
        schematizer,
        high_priority_real_refresh,
        real_refresh_source_two,
        fake_source,
        fake_source_two,
        fake_cluster,
        fake_database,
        mock_popen
    ):
        refresh_manager.total_throughput_cap = (DEFAULT_CAP * 2) - 30
        expected_job = self._create_job_from_refresh(
            real_refresh_source_two,
            throughput=DEFAULT_CAP - 30,
            last_throughput=0,
            pid=self.pid
        )
        expected_high_priority_job = self._create_job_from_refresh(
            high_priority_real_refresh,
            throughput=DEFAULT_CAP,
            last_throughput=0,
            pid=self.pid
        )
        refresh_manager.step()
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_high_priority_job],
            fake_source_two.name: [expected_job]
        }
        assert refresh_manager.sort_sources_by_top_refresh() == [
            fake_source.name, fake_source_two.name
        ]
        assert refresh_manager.refresh_queue.peek() == {}
        assert mock_popen.call_count == 2
        mock_popen.assert_any_call(
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(DEFAULT_CAP),
             '--batch-size={}'.format(high_priority_real_refresh.batch_size),
             '--refresh-id={}'.format(high_priority_real_refresh.refresh_id),
             '--dry-run', '--primary=id'
             ]
        )
        mock_popen.assert_any_call(
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source_two.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(DEFAULT_CAP - 30),
             '--batch-size={}'.format(real_refresh_source_two.batch_size),
             '--refresh-id={}'.format(real_refresh_source_two.refresh_id),
             '--dry-run',
             '--where="country=\'CA\' AND city=\'Waterloo\'"',
             '--primary=id'
             ]
        )

    def test_two_job_in_source(
        self,
        refresh_manager,
        schematizer,
        real_refresh,
        fake_source,
        fake_cluster,
        fake_database,
        mock_popen,
        mock_kill
    ):
        refresh_manager.per_source_throughput_cap = 60
        expected_job = self._create_job_from_refresh(
            real_refresh,
            throughput=DEFAULT_CAP,
            last_throughput=0,
            pid=self.pid
        )
        refresh_manager.step()
        mock_popen.assert_called_once_with(
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(60),
             '--batch-size={}'.format(real_refresh.batch_size),
             '--refresh-id={}'.format(real_refresh.refresh_id),
             '--dry-run', '--primary=id'
             ]
        )
        assert mock_kill.call_count == 0
        schematizer.update_refresh(
            real_refresh.refresh_id,
            status=RefreshStatus.IN_PROGRESS,
            offset=0
        )
        new_refresh = schematizer.create_refresh(
            source_id=fake_source.source_id,
            offset=0,
            batch_size=500,
            priority=Priority.HIGH.value,
            filter_condition=None,
            avg_rows_per_second_cap=None
        )

        mock_kill.side_effect = lambda pid, signal: schematizer.update_refresh(
            real_refresh.refresh_id,
            status=RefreshStatus.PAUSED,
            offset=500
        )

        refresh_manager.step()

        expected_job = self._create_job_from_refresh(
            real_refresh,
            throughput=60 - DEFAULT_CAP,
            last_throughput=60,
            pid=self.pid,
            status=RefreshStatus.IN_PROGRESS
        )
        expected_new_job = self._create_job_from_refresh(
            new_refresh,
            throughput=DEFAULT_CAP,
            last_throughput=0,
            pid=self.pid
        )

        mock_kill.assert_called_once_with(
            self.pid, signal.SIGTERM
        )
        mock_popen.assert_any_call(
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=500',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(60 - DEFAULT_CAP),
             '--batch-size={}'.format(real_refresh.batch_size),
             '--refresh-id={}'.format(real_refresh.refresh_id),
             '--dry-run', '--primary=id'
             ]
        )
        mock_popen.assert_any_call(
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(DEFAULT_CAP),
             '--batch-size={}'.format(new_refresh.batch_size),
             '--refresh-id={}'.format(new_refresh.refresh_id),
             '--dry-run', '--primary=id'
             ]
        )
        assert mock_popen.call_count == 3
        assert refresh_manager.active_refreshes == {
            fake_source.name: [expected_new_job, expected_job]
        }
