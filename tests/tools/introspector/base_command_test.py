# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
        "source_id",
        "source_name",
        "namespace"
    ]
)


@pytest.mark.usefixtures('containers')
class TestBaseCommand(TestIntrospectorBase):

    @pytest.fixture
    def command(self, containers):
        command = IntrospectorCommand("data_pipeline_introspector_base")
        command.log.debug = mock.Mock()
        command.log.info = mock.Mock()
        command.log.warning = mock.Mock()
        return command

    def test_active_sources(
        self,
        command,
        active_topics,
        inactive_topics
    ):
        actual_active_sources = command.active_sources
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
        command,
        namespaces
    ):
        actual_active_namespaces = command.active_namespaces
        for namespace in namespaces:
            assert namespace in actual_active_namespaces.keys()
            assert actual_active_namespaces[
                namespace
            ]['active_topic_count'] == 1
            assert actual_active_namespaces[
                namespace
            ]['active_source_count'] == 1

    def test_list_topics_source_id_no_sort(
        self,
        command,
        topic_one_active,
        source_one_active,
        namespace_one
    ):
        actual_topics = command.list_topics(
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
        command,
        topic_one_inactive,
        topic_one_inactive_b
    ):
        actual_topics = command.list_topics(
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
        command,
        source_one_active,
        namespace_one,
        topic_one_active
    ):
        actual_topics = command.list_topics(
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
        command,
        namespace_one,
        topic_one_active,
        topic_one_inactive,
        source_one_active,
        source_one_inactive
    ):
        actual_sources = command.list_sources(
            namespace_name=namespace_one,
            active_sources=True,
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
        command,
        sources
    ):
        actual_sources = command.list_sources()
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
        command,
        namespace_one,
        namespace_two,
        topic_one_active,
        topic_two_active
    ):
        namespace_one_obj = topic_one_active.source.namespace
        namespace_two_obj = topic_two_active.source.namespace
        actual_namespaces = command.list_namespaces(
            active_namespaces=True,
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

    def test_process_source_and_namespace_args_names(
        self,
        command,
        parser,
        namespace_one,
        source_one_active
    ):
        args = Args(
            source_id=None,
            source_name=source_one_active,
            namespace=namespace_one
        )
        command.process_source_and_namespace_args(args, parser)
        assert command.source_name == source_one_active
        assert command.namespace == namespace_one
        assert not command.source_id

    def test_process_source_and_namespace_args_name_error(
        self,
        command,
        parser,
        source_one_active
    ):
        args = Args(
            source_id=None,
            source_name=source_one_active,
            namespace=None
        )
        with pytest.raises(FakeParserError) as e:
            command.process_source_and_namespace_args(args, parser)
        assert e
        assert e.value.args
        assert "--namespace must be provided" in e.value.args[0]

    def test_process_source_and_namespace_args_id_with_warning(
        self,
        command,
        parser,
        source_one_active,
        namespace_one,
        namespace_two,
        topic_one_active
    ):
        source_id = topic_one_active.source.source_id
        args = Args(
            source_id=source_id,
            source_name=None,
            namespace=namespace_two
        )
        command.process_source_and_namespace_args(args, parser)
        command.log.warning.assert_called_once_with(
            "Since source id was given, --namespace will be ignored"
        )
        assert command.source_name == source_one_active
        assert command.namespace == namespace_one
