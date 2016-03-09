# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import mock
import pytest
from kazoo.exceptions import NoNodeError
from yelp_kafka_tool.util.zookeeper import ZK

from data_pipeline.tools.compaction_setter import CompactionSetter


class TestCompactionSetter(object):

    @pytest.fixture
    def batch(self):
        batch = CompactionSetter()
        return batch

    @pytest.fixture
    def namespace(self):
        Namespace = namedtuple("Namespace", ["name"])
        return Namespace("some_namespace")

    @pytest.fixture
    def topic_name(self):
        return "some_topic"

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
            return_value=fake_topic_config
        ) as mock_get_topic_config:
            yield mock_get_topic_config

    @pytest.yield_fixture
    def mock_get_topic_config_with_cleanup_policy(self, fake_topic_config_with_cleanup_policy):
        with mock.patch.object(
            ZK,
            'get_topic_config',
            return_value=fake_topic_config_with_cleanup_policy
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

    def _create_mock_schematizer(self, namespaces, topics, filter_topics):
        mock_schematizer = mock.Mock()
        mock_schematizer.get_namespaces = mock.Mock(
            return_value=namespaces
        )
        mock_schematizer.get_topics_by_criteria = mock.Mock(
            return_value=topics
        )
        mock_schematizer.filter_topics_by_pkeys = mock.Mock(
            return_value=filter_topics
        )
        return mock_schematizer

    @pytest.yield_fixture
    def mock_get_schematizer(self, topic, topic_name, namespace):
        with mock.patch(
            'data_pipeline.tools.compaction_setter.get_schematizer'
        ) as mock_get_schematizer:
            mock_get_schematizer.return_value = self._create_mock_schematizer(
                [namespace],
                [topic],
                [topic_name]
            )
            yield mock_get_schematizer

    @pytest.yield_fixture
    def mock_get_schematizer_filtered_out(self, topic, namespace):
        with mock.patch(
            'data_pipeline.tools.compaction_setter.get_schematizer'
        ) as mock_get_schematizer:
            mock_get_schematizer.return_value = self._create_mock_schematizer(
                [namespace],
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

    def test_compact(
        self,
        batch,
        topic_name,
        fake_topic_config_with_cleanup_policy,
        mock_get_topic_config,
        mock_get_schematizer,
        mock_set_topic_config
    ):
        self._run_batch(batch)
        assert batch.compacted_topics == [topic_name]
        assert batch.skipped_topics == []
        assert batch.missed_topics == []
        mock_set_topic_config.assert_called_once_with(
            topic=topic_name,
            value=fake_topic_config_with_cleanup_policy
        )

    def test_filtered(
        self,
        batch,
        topic_name,
        mock_get_schematizer_filtered_out,
        mock_set_topic_config
    ):
        self._run_batch(batch)
        assert batch.compacted_topics == []
        assert batch.skipped_topics == []
        assert batch.missed_topics == []
        assert mock_set_topic_config.call_count == 0

    def test_skip(
        self,
        batch,
        topic_name,
        mock_get_topic_config_with_cleanup_policy,
        mock_get_schematizer,
        mock_set_topic_config
    ):
        self._run_batch(batch)
        assert batch.compacted_topics == []
        assert batch.skipped_topics == [topic_name]
        assert batch.missed_topics == []
        assert mock_set_topic_config.call_count == 0

    def test_miss(
        self,
        batch,
        topic_name,
        mock_get_topic_config_with_nonode,
        mock_get_schematizer,
        mock_set_topic_config
    ):
        self._run_batch(batch)
        assert batch.compacted_topics == []
        assert batch.skipped_topics == []
        assert batch.missed_topics == [topic_name]
        assert mock_set_topic_config.call_count == 0

    def _run_batch(self, batch):
        batch.process_commandline_options([])
        batch.run()
