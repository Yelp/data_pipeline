# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline.tools.introspector.info.namespace import NamespaceInfoCommand
from data_pipeline.tools.introspector.info.source import SourceInfoCommand
from data_pipeline.tools.introspector.info.topic import TopicInfoCommand
from tests.tools.introspector.base_test import TestIntrospectorBase


class TestTopicInfoCommand(TestIntrospectorBase):

    @pytest.fixture
    def command(self, containers):
        command = TopicInfoCommand("data_pipeline_introspector_info_topic")
        command.log.debug = mock.Mock()
        command.log.info = mock.Mock()
        command.log.warning = mock.Mock()
        return command

    def test_list_schemas(
        self,
        command,
        schematizer,
        topic_one_active,
        schema_one_active
    ):
        actual_schemas = command.list_schemas(topic_one_active.name)
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        assert len(actual_schemas) == 1
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schemas[0]
        )

    def test_info_topic(
        self,
        command,
        schematizer,
        schema_one_active,
        namespace_one,
        source_one_active,
        topic_one_active
    ):
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        topic_dict = command.info_topic(topic_one_active.name)
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=topic_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )
        actual_schemas = topic_dict['schemas']
        assert len(actual_schemas) == 1
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schemas[0]
        )


class TestSourceInfoCommand(TestIntrospectorBase):

    @pytest.fixture
    def command(self, containers):
        command = SourceInfoCommand("data_pipeline_introspector_info_source")
        command.log.debug = mock.Mock()
        command.log.info = mock.Mock()
        command.log.warning = mock.Mock()
        return command

    def test_info_source_id(
        self,
        command,
        topic_one_inactive,
        topic_one_inactive_b,
        source_one_inactive,
        namespace_one
    ):
        source_obj = topic_one_inactive.source
        source_dict = command.info_source(
            source_id=source_obj.source_id,
            source_name=None,
            namespace_name=None,
            active_sources=True
        )
        self._assert_source_equals_source_dict(
            source=source_obj,
            source_dict=source_dict,
            namespace_name=namespace_one,
            source_name=source_one_inactive,
            active_topic_count=0
        )
        source_topics = source_dict['topics']
        assert len(source_topics) == 2

        sorted_expected_topics = sorted(
            [topic_one_inactive, topic_one_inactive_b],
            key=lambda topic: topic.topic_id
        )
        sorted_actual_topics = sorted(
            source_topics,
            key=lambda topic_dict: topic_dict['topic_id']
        )
        self._assert_topic_equals_topic_dict(
            topic=sorted_expected_topics[0],
            topic_dict=sorted_actual_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_inactive,
            is_active=False
        )
        self._assert_topic_equals_topic_dict(
            topic=sorted_expected_topics[1],
            topic_dict=sorted_actual_topics[1],
            namespace_name=namespace_one,
            source_name=source_one_inactive,
            is_active=False
        )

    def test_info_source_missing_source_name(
        self,
        command,
        namespace_one
    ):
        with pytest.raises(ValueError) as e:
            command.info_source(
                source_id=None,
                source_name="this_source_will_not_exist",
                namespace_name=namespace_one
            )
        assert e.value.args
        assert "Given SOURCE_NAME|NAMESPACE_NAME doesn't exist" in e.value.args[0]

    def test_info_source_name_pair(
        self,
        command,
        topic_one_active,
        source_one_active,
        namespace_one
    ):
        source_dict = command.info_source(
            source_id=None,
            source_name=source_one_active,
            namespace_name=namespace_one,
            active_sources=True
        )
        source_obj = topic_one_active.source
        self._assert_source_equals_source_dict(
            source=source_obj,
            source_dict=source_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            active_topic_count=1
        )
        source_topics = source_dict['topics']
        assert len(source_topics) == 1
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=source_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )


class TestNamespaceInfoCommand(TestIntrospectorBase):

    @pytest.fixture
    def command(self, containers):
        command = NamespaceInfoCommand("data_pipeline_introspector_info_namespace")
        command.log.debug = mock.Mock()
        command.log.info = mock.Mock()
        command.log.warning = mock.Mock()
        return command

    def test_info_namespace_missing(
        self,
        command
    ):
        with pytest.raises(ValueError) as e:
            command.info_namespace("not_a_namespace")
        assert e.value.args
        assert "Given namespace doesn't exist" in e.value.args[0]

    def test_info_namespace(
        self,
        command,
        namespace_two,
        source_two_inactive,
        source_two_active,
        topic_two_active
    ):
        namespace_obj = topic_two_active.source.namespace
        namespace_dict = command.info_namespace(
            namespace_two,
            active_namespaces=True
        )
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_obj,
            namespace_dict=namespace_dict,
            namespace_name=namespace_two
        )
        namespace_sources = namespace_dict['sources']
        assert len(namespace_sources) == 2
        if namespace_sources[0]['name'] == source_two_active:
            assert namespace_sources[1]['name'] == source_two_inactive
        else:
            assert namespace_sources[0]['name'] == source_two_inactive
            assert namespace_sources[1]['name'] == source_two_active
