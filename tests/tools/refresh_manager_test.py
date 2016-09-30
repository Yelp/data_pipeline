# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from collections import namedtuple
from datetime import datetime
from uuid import uuid4

import mock
import simplejson
import psutil
import pytest

import data_pipeline.tools.refresh_manager
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.schematizer_clientlib.models import refresh
from data_pipeline.tools.refresh_manager import FullRefreshManager
from data_pipeline.tools.refresh_manager import PriorityRefreshQueue


@pytest.mark.usefixures('containers')
class TestFullRefreshManager(object):

    def _create_schema(self, namespace, source):
        return simplejson.dumps(
        {
            "type": "record",
            "namespace": namespace,
            "name": source,
            "doc": "test",
            "fields":[
                {"type": "int", "name": "field", "doc": "test_field", "default": 1}
            ]
        })

    @property
    def config_path(self):
        return '/nail/srv/configs/data_pipeline_tools.yaml'

    @property
    def source_owner_email(self):
        return "bam+test+introspection@yelp.com"

    @pytest.fixture(scope='class')
    def schematizer(self, containers):
        return get_schematizer()

    @pytest.fixture(scope='class')
    def fake_namespace(self):
        return "refresh_primary.yelp"

    @pytest.fixture(scope='class')
    def fake_source_name(self):
        return 'manager_source_{}'.format(uuid4())

    @pytest.fixture(scope='class')
    def fake_source_two_name(self):
        return 'manager_source_two_{}'.format(uuid4())

    @pytest.fixture(scope='class')
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

    @pytest.fixture(autouse=True, scope='class')
    def fake_source(self, fake_topic):
        return fake_topic.source

    @pytest.fixture(scope='class')
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

    @pytest.fixture(autouse=True, scope='class')
    def fake_source(self, fake_topic_two):
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
        refresh_params['priority'] = refresh.Priority.MEDIUM.value
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def paused_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 2
        refresh_params['status'] = 'PAUSED'
        refresh_params['priority'] = refresh.Priority.MEDIUM.value
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def high_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 3
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = refresh.Priority.HIGH.value
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def complete_refresh_result(self, refresh_params):
        refresh_params['refresh_id'] = 4
        refresh_params['status'] = 'SUCCESS'
        refresh_params['priority'] = refresh.Priority.MEDIUM.value
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def refresh_result_two(self, refresh_params, fake_source_two_name):
        refresh_params['source_name'] = fake_source_two_name
        refresh_params['refresh_id'] = 5
        refresh_params['status'] = 'NOT_STARTED'
        refresh_params['priority'] = refresh.Priority.MEDIUM.value
        return refresh._Refresh(**refresh_params).to_result()

    @pytest.fixture
    def refresh_manager(self, fake_namespace):
        refresh_manager = FullRefreshManager()
        refresh_manager.options = namedtuple('Options', ['namespace', 'config_path', 'dry_run'])(
            fake_namespace,
            self.config_path,
            True
        )
        refresh_manager._init_global_state()
        return refresh_manager

    @pytest.fixture
    def priority_refresh_queue(self):
        return PriorityRefreshQueue()

    def test_refresh_queue_single_job(
        self,
        priority_refresh_queue,
        fake_source_name,
        refresh_result
    ):
        assert priority_refresh_queue.peek() == []
        priority_refresh_queue.update([refresh_result])
        assert priority_refresh_queue.peek() == [(fake_source_name, refresh_result)]
        assert priority_refresh_queue.pop(fake_source_name) == refresh_result
        assert priority_refresh_queue.peek() == []
        assert priority_refresh_queue.refresh_ref == {}
        assert priority_refresh_queue.source_to_refresh_queue == {}

    def test_refresh_queue_priority_and_paused_sort(
        self,
        priority_refresh_queue,
        fake_source_name,
        refresh_result,
        paused_refresh_result,
        high_refresh_result
    ):
        priority_refresh_queue.update(
            [paused_refresh_result, refresh_result, high_refresh_result]
        )
        assert priority_refresh_queue.peek() == [(fake_source_name, high_refresh_result)]
        assert priority_refresh_queue.pop(fake_source_name) == high_refresh_result
        assert priority_refresh_queue.peek() == [(fake_source_name, paused_refresh_result)]
        assert priority_refresh_queue.pop(fake_source_name) == paused_refresh_result
        assert priority_refresh_queue.peek() == [(fake_source_name, refresh_result)]

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
        assert (fake_source_name, refresh_result) in refresh_set
        assert (fake_source_two_name, refresh_result_two) in refresh_set
        assert len(refresh_set) == 2
        assert priority_refresh_queue.pop(fake_source_name) == refresh_result
        assert priority_refresh_queue.peek() == [(fake_source_two_name, refresh_result_two)]

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
        assert fake_topic.name == \
            refresh_manager.get_most_recent_topic_name(fake_source_name, fake_namespace)

    def test_job_construction(
        self,
        refresh_manager,
        schematizer,
        fake_source,
        fake_namespace
    ):
        refresh = schematizer.create_refresh(
            source_id=fake_source.source_id,
            offset=0,
            batch_size=500,
            priority=refresh.Priority.MEDIUM.value,
            filter_condition=None,
            avg_rows_per_second_cap=None
        )
