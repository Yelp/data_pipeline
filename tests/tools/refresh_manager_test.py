# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import mock
import psutil
import pytest

from data_pipeline.batch.refresh_manager import FullRefreshManager
from data_pipeline.schematizer_clientlib.models import namespace
from data_pipeline.schematizer_clientlib.models import refresh
from data_pipeline.schematizer_clientlib.models import source


class TestFullRefreshManager(object):

    @pytest.fixture
    def fake_namespace(self):
        return 'fake_namespace'

    @pytest.fixture
    def fake_source(self):
        return 'fake_source'

    @pytest.fixture
    def fake_created_at(self):
        return datetime(2015, 1, 1, 17, 0, 0)

    @pytest.fixture
    def fake_updated_at(self):
        return datetime(2015, 1, 1, 17, 0, 1)

    @pytest.fixture
    def refresh_params(
        self,
        source_response,
        fake_created_at,
        fake_updated_at
    ):
        return {
            'source': source_response,
            'offset': 0,
            'batch_size': 200,
            'filter_condition': None,
            'created_at': fake_created_at,
            'updated_at': fake_updated_at
        }

    @pytest.fixture
    def namespace_response(self, fake_namespace):
        return namespace._Namespace(
            namespace_id=1,
            name=fake_namespace
        )

    @pytest.fixture
    def source_response(self, namespace_response, fake_source):
        return source._Source(
            source_id=1,
            name=fake_source,
            owner_email='fake_email@yelp.com',
            namespace=namespace_response
        )

    @pytest.fixture
    def refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 1
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = 'MEDIUM'
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def high_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 2
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = 'HIGH'
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def complete_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 3
        refresh_params['status'] = 'SUCCESS'
        refresh_params['priority'] = 'MEDIUM'
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.yield_fixture
    def mock_config(self):
        with mock.patch(
            'data_pipeline.batch.refresh_manager.load_default_config'
        ), mock.patch(
            'data_pipeline.batch.refresh_manager.get_schematizer'
        ):
            yield

    @pytest.fixture
    def refresh_manager(self, mock_config, fake_namespace):
        refresh_manager = FullRefreshManager()
        refresh_manager.namespace_name = fake_namespace
        return refresh_manager

    @pytest.yield_fixture
    def mock_setup(self, refresh_manager):
        with mock.patch.object(
            refresh_manager,
            'setup_new_refresh'
        ) as mock_setup:
            yield mock_setup

    def test_process_next_refresh(
        self,
        refresh_manager,
        mock_setup,
        refresh_result
    ):
        refresh_manager.process_next_refresh(refresh_result)
        mock_setup.assert_called_once_with(refresh_result)
        assert refresh_manager.schematizer.get_refresh_by_id.call_count == 0

    def test_process_refresh_replace_active_refresh(
        self,
        refresh_manager,
        mock_setup,
        refresh_result,
        high_refresh_result
    ):
        refresh_manager.active_refresh['id'] = 1
        mock_schematizer = refresh_manager.schematizer
        mock_schematizer.get_refresh_by_id.return_value = refresh_result
        refresh_manager.process_next_refresh(high_refresh_result)
        mock_schematizer.get_refresh_by_id.assert_called_once_with(1)
        mock_setup.assert_called_once_with(high_refresh_result)

    def test_process_refresh_no_replace(
        self,
        refresh_manager,
        mock_setup,
        refresh_result,
        high_refresh_result
    ):
        refresh_manager.active_refresh['id'] = 2
        mock_schematizer = refresh_manager.schematizer
        mock_schematizer.get_refresh_by_id.return_value = high_refresh_result
        refresh_manager.process_next_refresh(refresh_result)
        mock_schematizer.get_refresh_by_id.assert_called_once_with(2)
        assert mock_setup.call_count == 0

    def test_process_refresh_completed_refresh(
        self,
        refresh_manager,
        mock_setup,
        refresh_result,
        complete_refresh_result
    ):
        refresh_manager.active_refresh['id'] = 3
        schematizer = refresh_manager.schematizer
        schematizer.get_refresh_by_id.return_value = complete_refresh_result
        refresh_manager.process_next_refresh(refresh_result)
        schematizer.get_refresh_by_id.assert_called_once_with(3)
        mock_setup.assert_called_once_with(refresh_result)

    def test_determine_best_refresh(
        self,
        refresh_manager,
        refresh_result,
        high_refresh_result
    ):
        medium_result_list = [refresh_result]
        high_result_list = [high_refresh_result]
        best_refresh = refresh_manager.determine_best_refresh(
            medium_result_list,
            high_result_list
        )
        assert best_refresh == high_refresh_result

    def test_determine_best_refresh_both_empty(self, refresh_manager):
        result_list_a = []
        result_list_b = []
        best_refresh = refresh_manager.determine_best_refresh(
            result_list_a,
            result_list_b
        )
        assert best_refresh is None

    def test_determine_best_refresh_first_empty(
        self,
        refresh_manager,
        refresh_result
    ):
        result_list_a = []
        result_list_b = [refresh_result]
        best_refresh = refresh_manager.determine_best_refresh(
            result_list_a,
            result_list_b
        )
        assert best_refresh == refresh_result

    def test_determine_best_refresh_second_empty(
        self,
        refresh_manager,
        refresh_result
    ):
        result_list_a = [refresh_result]
        result_list_b = []
        best_refresh = refresh_manager.determine_best_refresh(
            result_list_a,
            result_list_b
        )
        assert best_refresh == refresh_result

    def test_handle_zombie_refreshes(self, refresh_manager):
        refresh_manager.active_refresh['id'] = 1
        refresh_manager.active_refresh['pid'] = 1
        with mock.patch(
            'data_pipeline.batch.refresh_manager.psutil.Process',
        ) as mock_ps:
            mock_ps.return_value.status.return_value = psutil.STATUS_ZOMBIE
            mock_sch = refresh_manager.schematizer
            mock_sch.get_refresh_by_id.return_value.status = 'IN_PROGRESS'
            refresh_manager.handle_zombie_refreshes()
            mock_sch.get_refresh_by_id.assert_called_once_with(1)
            mock_sch.update_refresh.assert_called_once_with(1, 'FAILED', 0)

    def test_get_next_refresh(self, refresh_manager, refresh_result):
        with mock.patch.object(
            refresh_manager,
            'determine_best_refresh'
        ) as mock_determine:
            not_started = [refresh_result]
            paused = []
            mock_schematizer = refresh_manager.schematizer
            mock_schematizer.get_refreshes_by_criteria.side_effect = [
                not_started,
                paused
            ]
            refresh_manager.get_next_refresh()
            mock_determine.assert_called_once_with(not_started, paused)
