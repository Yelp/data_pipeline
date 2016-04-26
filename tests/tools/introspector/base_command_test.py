# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import mock
import pytest

from data_pipeline.tools.introspector.base_command import IntrospectorCommand
from tests.tools.introspector.base_test import FakeParserError
from tests.tools.introspector.base_test import TestIntrospectorBase


Args = namedtuple(
    "Namespace", [
        "source",
        "namespace"
    ]
)


@pytest.mark.usefixtures('containers')
class TestBaseCommand(TestIntrospectorBase):

    @pytest.fixture
    def batch(self, containers):
        batch = IntrospectorCommand("data_pipeline_introspector_base")
        batch.log.debug = mock.Mock()
        batch.log.info = mock.Mock()
        batch.log.warning = mock.Mock()
        return batch

    def test_active_sources(
        self,
        batch,
        active_topics,
        inactive_topics
    ):
        actual_active_sources = batch.active_sources
        active_sources = [topic.source for topic in active_topics]
        inactive_sources = [topic.source for topic in inactive_topics]
        for source in active_sources:
            assert source.source_id in actual_active_sources.keys()
            assert actual_active_sources[
                source.source_id
            ]['active_topic_count'] == 1
        for source in inactive_sources:
            assert source.source_id not in actual_active_sources.keys()

    def test_active_namespaces(
        self,
        batch,
        namespaces
    ):
        actual_active_namespaces = batch.active_namespaces
        for namespace in namespaces:
            assert namespace in actual_active_namespaces.keys()
            assert actual_active_namespaces[
                namespace
            ]['active_topic_count'] == 1
            assert actual_active_namespaces[
                namespace
            ]['active_source_count'] == 1

    def test_list_schemas(
        self,
        batch,
        schematizer,
        topic_one_active,
        schema_one_active
    ):
        actual_schemas = batch.list_schemas(topic_one_active.name)
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        assert len(actual_schemas) == 1
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schemas[0]
        )

    def test_list_topics_source_id_no_sort(
        self,
        batch,
        topic_one_active,
        source_one_active,
        namespace_one
    ):
        actual_topics = batch.list_topics(
            source_id=topic_one_active.source.source_id
        )
        assert len(actual_topics) == 1
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=actual_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )

    def test_list_topics_source_id_sort(
        self,
        batch,
        topic_one_inactive,
        topic_one_inactive_b
    ):
        actual_topics = batch.list_topics(
            source_id=topic_one_inactive.source.source_id,
            sort_by='name',
            descending_order=True
        )
        assert len(actual_topics) == 2
        a_name = topic_one_inactive.name
        b_name = topic_one_inactive_b.name
        a_pos = -1
        b_pos = -1
        for i, topic_dict in enumerate(actual_topics):
            if topic_dict['name'] == a_name:
                a_pos = i
            elif topic_dict['name'] == b_name:
                b_pos = i
        assert a_pos != -1
        assert b_pos != -1
        assert (
            (a_pos < b_pos and a_name > b_name) or
            (a_pos > b_pos and a_name < b_name)
        )

    def test_list_topics_namespace_source_names_no_sort(
        self,
        batch,
        source_one_active,
        namespace_one,
        topic_one_active
    ):
        actual_topics = batch.list_topics(
            namespace_name=namespace_one,
            source_name=source_one_active
        )
        assert len(actual_topics) == 1
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=actual_topics[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )

    def test_list_sources_namespace_name_sort(
        self,
        batch,
        namespace_one,
        topic_one_active,
        topic_one_inactive,
        source_one_active,
        source_one_inactive
    ):
        actual_sources = batch.list_sources(
            namespace_name=namespace_one,
            sort_by='name'
        )
        active_source_obj = topic_one_active.source
        inactive_source_obj = topic_one_inactive.source
        assert len(actual_sources) == 2
        self._assert_source_equals_source_dict(
            source=active_source_obj,
            source_dict=actual_sources[0],
            namespace_name=namespace_one,
            source_name=source_one_active,
            active_topic_count=1
        )
        self._assert_source_equals_source_dict(
            source=inactive_source_obj,
            source_dict=actual_sources[1],
            namespace_name=namespace_one,
            source_name=source_one_inactive,
            active_topic_count=0
        )

    def test_list_sources_no_name_no_sort(
        self,
        batch,
        sources
    ):
        actual_sources = batch.list_sources()
        found_sources = [False for source in sources]
        for actual_source in actual_sources:
            source_name = actual_source['name']
            for i, expected_source in enumerate(sources):
                if expected_source == source_name:
                    assert not found_sources[i]
                    found_sources[i] = True
                    break
        for i, found_result in enumerate(found_sources):
            if not found_result:
                assert not "Could not find {} when listing all namespaces".format(sources[i])

    def test_list_namespaces(
        self,
        batch,
        namespace_one,
        namespace_two,
        topic_one_active,
        topic_two_active
    ):
        namespace_one_obj = topic_one_active.source.namespace
        namespace_two_obj = topic_two_active.source.namespace
        actual_namespaces = batch.list_namespaces(
            sort_by='name'
        )
        one_pos = -1
        two_pos = -1
        for i, namespace_dict in enumerate(actual_namespaces):
            namespace_name = namespace_dict['name']
            if namespace_name == namespace_one:
                assert one_pos == -1
                one_pos = i
                self._assert_namespace_equals_namespace_dict(
                    namespace=namespace_one_obj,
                    namespace_name=namespace_one,
                    namespace_dict=namespace_dict
                )
            elif namespace_name == namespace_two:
                assert two_pos == -1
                two_pos = i
                self._assert_namespace_equals_namespace_dict(
                    namespace=namespace_two_obj,
                    namespace_name=namespace_two,
                    namespace_dict=namespace_dict
                )
        assert one_pos != -1
        assert two_pos != -1
        assert one_pos < two_pos

    def test_info_topic(
        self,
        batch,
        schematizer,
        schema_one_active,
        namespace_one,
        source_one_active,
        topic_one_active
    ):
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        topic_dict = batch.info_topic(topic_one_active.name)
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

    def test_info_source_id(
        self,
        batch,
        topic_one_inactive,
        topic_one_inactive_b,
        source_one_inactive,
        namespace_one
    ):
        source_obj = topic_one_inactive.source
        source_dict = batch.info_source(
            source_id=source_obj.source_id,
            source_name=None,
            namespace_name=None
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
        batch,
        namespace_one
    ):
        with pytest.raises(ValueError) as e:
            batch.info_source(
                source_id=None,
                source_name="this_source_will_not_exist",
                namespace_name=namespace_one
            )
        assert e.value.args
        assert "Given SOURCE_NAME|NAMESPACE_NAME doesn't exist" in e.value.args[0]

    def test_info_source_name_pair(
        self,
        batch,
        topic_one_active,
        source_one_active,
        namespace_one
    ):
        source_dict = batch.info_source(
            source_id=None,
            source_name=source_one_active,
            namespace_name=namespace_one
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

    def test_info_namespace_missing(
        self,
        batch
    ):
        with pytest.raises(ValueError) as e:
            batch.info_namespace("not_a_namespace")
        assert e.value.args
        assert "Given namespace doesn't exist" in e.value.args[0]

    def test_info_namespace(
        self,
        batch,
        namespace_two,
        source_two_inactive,
        source_two_active,
        topic_two_active
    ):
        namespace_obj = topic_two_active.source.namespace
        namespace_dict = batch.info_namespace(namespace_two)
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

    def test_process_source_and_namespace_args_names(
        self,
        batch,
        parser,
        namespace_one,
        source_one_active
    ):
        args = Args(
            source=source_one_active,
            namespace=namespace_one
        )
        batch.process_source_and_namespace_args(args, parser)
        assert batch.source_name == source_one_active
        assert batch.namespace == namespace_one
        assert not batch.source_id

    def test_process_source_and_namespace_args_name_error(
        self,
        batch,
        parser,
        source_one_active
    ):
        args = Args(
            source=source_one_active,
            namespace=None
        )
        with pytest.raises(FakeParserError) as e:
            batch.process_source_and_namespace_args(args, parser)
        assert e
        assert e.value.args
        assert "--namespace must be provided" in e.value.args[0]

    def test_process_source_and_namespace_args_id_with_warning(
        self,
        batch,
        parser,
        source_one_active,
        namespace_one,
        namespace_two,
        topic_one_active
    ):
        source_id = topic_one_active.source.source_id
        args = Args(
            source=str(source_id),
            namespace=namespace_two
        )
        batch.process_source_and_namespace_args(args, parser)
        batch.log.warning.assert_called_once_with(
            "Since source id was given, --namespace will be ignored"
        )
        assert batch.source_name == source_one_active
        assert batch.namespace == namespace_one
