# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import pytest

from data_pipeline.schematizer_clientlib.models.refresh import _Refresh
from data_pipeline.schematizer_clientlib.models.refresh import Priority
from data_pipeline.tools.refresh_manager import PriorityRefreshQueue


@pytest.mark.usefixures('containers')
class TestPriorityRefreshQueue(object):

    @pytest.fixture
    def fake_namespace(self):
        return "refresh_primary.yelp"

    @pytest.fixture
    def fake_source_name(self):
        return 'manager_source_{}'.format(uuid4())

    @pytest.fixture
    def fake_source_two_name(self):
        return 'manager_source_two_{}'.format(uuid4())

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
    def priority_refresh_queue(self):
        return PriorityRefreshQueue()

    def test_refresh_queue_single_job(
        self,
        priority_refresh_queue,
        fake_source_name,
        refresh_result
    ):
        assert priority_refresh_queue.peek() == {}
        priority_refresh_queue.add_refreshes_to_queue([refresh_result])
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
        priority_refresh_queue.add_refreshes_to_queue(
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
        priority_refresh_queue.add_refreshes_to_queue([refresh_result, refresh_result_two])
        refresh_set = priority_refresh_queue.peek()
        assert refresh_set[fake_source_name] == refresh_result
        assert refresh_set[fake_source_two_name] == refresh_result_two
        assert priority_refresh_queue.pop(fake_source_name) == refresh_result
        assert priority_refresh_queue.peek() == {fake_source_two_name: refresh_result_two}
