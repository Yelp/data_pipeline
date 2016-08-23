# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
from collections import namedtuple

import mock
import pytest
from kazoo.exceptions import NoNodeError

from data_pipeline.tools.compaction_setter import CompactionSetter
from data_pipeline.tools.compaction_setter import ZK


class TestCompactionSetter(object):

    @pytest.fixture
    def compaction_setter(self):
        return CompactionSetter()

    @pytest.fixture
    def real_schematizer_compaction_setter(self, containers):
        compaction_setter = CompactionSetter()
        compaction_setter.process_commandline_options(args=[])
        return compaction_setter

    @pytest.fixture
    def namespace(self):
        Namespace = namedtuple("Namespace", ["name"])
        return Namespace("some_namespace")

    @pytest.fixture
    def source(self):
        Source = namedtuple("Source", ["name"])
        return Source("some_source")

    @pytest.fixture
    def topic_name(self):
        return "some_topic_compaction_setter"

    @pytest.fixture
    def topic(self, topic_name):
        Topic = namedtuple("Topic", ["name"])
        return Topic(topic_name)

    @pytest.fixture
    def fake_topic_config(self):
        return {"version": 1, "config": {"min.isr": 3}}

    @pytest.fixture
    def fake_topic_config_with_cleanup_policy(self):
        return {"version": 1, "config": {"min.isr": 3, "cleanup.policy": "compact"}}

    @pytest.fixture
    def fake_topic_config_with_cleanup_policy_delete(self):
        return {"version": 1, "config": {"min.isr": 3, "cleanup.policy": "delete"}}

    @pytest.yield_fixture
    def mock_get_topic_config(self, fake_topic_config):
        with mock.patch.object(
            ZK,
            'get_topic_config',
            # We mutate these topic configs within the code, so we need to create copies
            # We can't use the deepcopy method itself since it doesn't take a parameter
            side_effect=lambda x: copy.deepcopy(fake_topic_config)
        ) as mock_get_topic_config:
            yield mock_get_topic_config

    @pytest.yield_fixture
    def mock_get_topic_config_with_cleanup_policy(self, fake_topic_config_with_cleanup_policy):
        with mock.patch.object(
            ZK,
            'get_topic_config',
            # We mutate these topic configs within the code, so we need to create copies
            # We can't use the deepcopy method itself since it doesn't take a parameter
            side_effect=lambda x: copy.deepcopy(fake_topic_config_with_cleanup_policy)
        ) as mock_get_topic_config:
            yield mock_get_topic_config

    @pytest.yield_fixture
    def mock_get_topic_config_with_nonode(self, fake_topic_config):
        with mock.patch.object(
            ZK,
            'get_topic_config',
            side_effect=NoNodeError
        ) as mock_get_topic_config:
            yield mock_get_topic_config

    @pytest.yield_fixture
    def mock_log_results(self):
        with mock.patch.object(
            CompactionSetter,
            'log_results',
            mock.Mock()
        ) as mock_log_results:
            yield mock_log_results

    def _create_mock_schematizer(self, namespaces, sources, topics, filter_topics):
        mock_schematizer = mock.Mock()
        mock_schematizer.get_namespaces = mock.Mock(
            return_value=namespaces
        )
        mock_schematizer.get_sources_by_namespace = mock.Mock(
            return_value=sources
        )
        mock_schematizer.get_topic_by_name = mock.Mock(
            return_value=topics[0]
        )
        mock_schematizer.get_topics_by_criteria = mock.Mock(
            return_value=topics
        )
        mock_schematizer.filter_topics_by_pkeys = mock.Mock(
            return_value=filter_topics
        )
        return mock_schematizer

    @pytest.yield_fixture
    def mock_get_schematizer(self, topic, topic_name, namespace, source):
        with mock.patch(
            'data_pipeline.tools.compaction_setter.get_schematizer'
        ) as mock_get_schematizer:
            mock_get_schematizer.return_value = self._create_mock_schematizer(
                [namespace],
                [source],
                [topic],
                [topic_name]
            )
            yield mock_get_schematizer

    @pytest.yield_fixture
    def mock_get_schematizer_filtered_out(self, topic, namespace, source):
        with mock.patch(
            'data_pipeline.tools.compaction_setter.get_schematizer'
        ) as mock_get_schematizer:
            mock_get_schematizer.return_value = self._create_mock_schematizer(
                [namespace],
                [source],
                [topic],
                []
            )
            yield mock_get_schematizer

    @pytest.yield_fixture
    def mock_set_topic_config(self):
        with mock.patch.object(
            ZK,
            'set_topic_config'
        ) as mock_set_topic_config:
            yield mock_set_topic_config

    @pytest.fixture
    def topic_resp(self, source, namespace, schematizer_client):
        pk_schema_json = {
            'type': 'record',
            'name': source.name,
            'namespace': namespace.name,
            'doc': 'test',
            'fields': [
                {'type': 'int', 'doc': 'test', 'name': 'id', 'pkey': 1},
                {'type': 'int', 'doc': 'test', 'name': 'data'}
            ],
            'pkey': ['id']
        }
        return schematizer_client.register_schema_from_schema_json(
            namespace=namespace.name,
            source=source.name,
            source_owner_email='bam+test@yelp.com',
            schema_json=pk_schema_json,
            contains_pii=False
        ).topic

    def test_compact(
        self,
        compaction_setter,
        topic_name,
        fake_topic_config_with_cleanup_policy,
        mock_get_topic_config,
        mock_get_schematizer,
        mock_set_topic_config,
        mock_log_results
    ):
        self._run_compaction_setter(compaction_setter)
        mock_log_results.assert_called_once_with(
            compacted_topics=[topic_name],
            skipped_topics=[],
            missed_topics=[]
        )
        mock_set_topic_config.assert_called_once_with(
            topic=topic_name,
            value=fake_topic_config_with_cleanup_policy
        )

    def test_compact_whitelisted_topic(
        self,
        compaction_setter,
        topic_name,
        fake_topic_config_with_cleanup_policy,
        mock_get_topic_config,
        mock_get_schematizer,
        mock_set_topic_config,
        mock_log_results
    ):
        compaction_setter.process_commandline_options(
            args=['--whitelist-topic={}'.format(topic_name)]
        )
        compaction_setter.run()
        mock_log_results.assert_called_once_with(
            compacted_topics=[topic_name],
            skipped_topics=[],
            missed_topics=[]
        )
        mock_set_topic_config.assert_called_once_with(
            topic=topic_name,
            value=fake_topic_config_with_cleanup_policy
        )

    def test_filtered(
        self,
        compaction_setter,
        topic_name,
        mock_get_schematizer_filtered_out,
        mock_set_topic_config,
        mock_log_results
    ):
        self._run_compaction_setter(compaction_setter)
        mock_log_results.assert_called_once_with(
            compacted_topics=[],
            skipped_topics=[],
            missed_topics=[]
        )
        assert mock_set_topic_config.call_count == 0

    def test_skip(
        self,
        compaction_setter,
        topic_name,
        mock_get_topic_config_with_cleanup_policy,
        mock_get_schematizer,
        mock_set_topic_config,
        mock_log_results
    ):
        self._run_compaction_setter(compaction_setter)
        mock_log_results.assert_called_once_with(
            compacted_topics=[],
            skipped_topics=[topic_name],
            missed_topics=[]
        )
        assert mock_set_topic_config.call_count == 0

    def test_miss(
        self,
        compaction_setter,
        topic_name,
        mock_get_topic_config_with_nonode,
        mock_get_schematizer,
        mock_set_topic_config,
        mock_log_results
    ):
        self._run_compaction_setter(compaction_setter)
        mock_log_results.assert_called_once_with(
            compacted_topics=[],
            skipped_topics=[],
            missed_topics=[topic_name]
        )
        assert mock_set_topic_config.call_count == 0

    def test_real_schematizer(
        self,
        real_schematizer_compaction_setter,
        topic_resp,
        containers,
        mock_get_topic_config,
        mock_set_topic_config,
        mock_log_results
    ):
        self._run_compaction_setter(real_schematizer_compaction_setter)
        kall = mock_log_results.call_args
        _, kwargs = kall
        assert topic_resp.name in kwargs['compacted_topics']
        assert topic_resp.name not in kwargs['skipped_topics']
        assert topic_resp.name not in kwargs['missed_topics']

    def test_real_schematizer_skip(
        self,
        real_schematizer_compaction_setter,
        topic_resp,
        containers,
        mock_get_topic_config_with_cleanup_policy,
        mock_set_topic_config,
        mock_log_results
    ):
        self._run_compaction_setter(real_schematizer_compaction_setter)
        kall = mock_log_results.call_args
        _, kwargs = kall
        assert topic_resp.name not in kwargs['compacted_topics']
        assert topic_resp.name in kwargs['skipped_topics']
        assert topic_resp.name not in kwargs['missed_topics']

    def _run_compaction_setter(self, compaction_setter):
        compaction_setter.process_commandline_options(args=[])
        compaction_setter.run()
