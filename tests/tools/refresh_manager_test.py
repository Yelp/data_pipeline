# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import signal
from collections import namedtuple
from uuid import uuid4

import mock
import pytest
import simplejson

from data_pipeline.schematizer_clientlib.models.refresh import Priority
from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.refresh_manager import FullRefreshManager

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

    @property
    def config_path(self):
        return '/nail/srv/configs/data_pipeline_tools.yaml'

    @property
    def source_owner_email(self):
        return "bam+test+refresh+manager@yelp.com"

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
    def refresh_manager(self, fake_cluster, fake_database):
        refresh_manager = FullRefreshManager()
        refresh_manager.options = namedtuple(
            'Options',
            ['cluster', 'database', 'config_path', 'dry_run', 'verbose',
             'per_source_throughput_cap', 'total_throughput_cap']
        )(
            fake_cluster,
            fake_database,
            self.config_path,
            True,
            0,
            DEFAULT_CAP,
            1000
        )
        refresh_manager._init_global_state()
        return refresh_manager

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

    def test_get_most_recent_topic_name(
        self,
        refresh_manager,
        fake_topic,
        fake_source_name,
        fake_namespace
    ):
        assert fake_topic == refresh_manager.get_most_recent_topic(
            fake_source_name, fake_namespace
        )

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
             '--dry-run', '--primary-key=id'
             ]
        )
        assert mock_kill.call_count == 0
        high_priority_real_refresh = schematizer.update_refresh(
            high_priority_real_refresh.refresh_id,
            RefreshStatus.IN_PROGRESS,
            0
        )
        refresh_manager.step()
        assert mock_kill.call_count == 0
        assert mock_popen.call_count == 1
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
             '--dry-run', '--primary-key=id'
             ]
        )
        assert mock_kill.call_count == 0
        schematizer.update_refresh(
            real_refresh.refresh_id,
            RefreshStatus.SUCCESS,
            0
        )
        refresh_manager.step()
        assert mock_kill.call_count == 0
        assert mock_popen.call_count == 2

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
        refresh_manager.step()
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
             '--dry-run', '--primary-key=id'
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
             '--primary-key=id'
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
             '--dry-run', '--primary-key=id'
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
        mock_kill.assert_called_once_with(
            self.pid, signal.SIGTERM
        )
        mock_popen.assert_called_with(
            ['python', refresh_manager.refresh_runner_path,
             '--cluster={}'.format(fake_cluster),
             '--database={}'.format(fake_database),
             '--table-name={}'.format(fake_source.name), '--offset=0',
             '--config-path={}'.format(refresh_manager.config_path),
             '--avg-rows-per-second-cap={}'.format(DEFAULT_CAP),
             '--batch-size={}'.format(new_refresh.batch_size),
             '--refresh-id={}'.format(new_refresh.refresh_id),
             '--dry-run', '--primary-key=id'
             ]
        )
        assert mock_popen.call_count == 2
