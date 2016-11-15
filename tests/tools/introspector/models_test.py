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

import mock
import pytest

from data_pipeline.tools.introspector.base_command import IntrospectorCommand
from data_pipeline.tools.introspector.models import IntrospectorNamespace
from data_pipeline.tools.introspector.models import IntrospectorSchema
from data_pipeline.tools.introspector.models import IntrospectorSource
from data_pipeline.tools.introspector.models import IntrospectorTopic
from tests.tools.introspector.base_test import TestIntrospectorBase


@pytest.mark.usefixtures('containers')
class TestIntrospectorModels(TestIntrospectorBase):

    @pytest.fixture
    def base_command(self, containers):
        command = IntrospectorCommand("data_pipeline_introspector_base")
        command.log.debug = mock.Mock()
        command.log.info = mock.Mock()
        command.log.warning = mock.Mock()
        return command

    def test_introspector_schema(
        self,
        schematizer,
        schema_one_active,
        topic_one_active
    ):
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        actual_schema_dict = IntrospectorSchema(
            topic_schema
        ).to_ordered_dict()
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schema_dict
        )

    def test_introspector_schema_with_topic_info(
        self,
        base_command,
        schematizer,
        schema_one_active,
        topic_one_active
    ):
        topic_schema = schematizer.get_latest_schema_by_topic_name(
            topic_one_active.name
        )
        actual_schema_dict = IntrospectorSchema(
            topic_schema,
            include_topic_info=True
        ).to_ordered_dict()
        self._assert_schema_equals_schema_dict(
            topic_schema=topic_schema,
            schema_obj=schema_one_active,
            schema_dict=actual_schema_dict,
            topic_to_check=topic_one_active
        )

    def test_introspector_topic(
        self,
        base_command,
        topic_one_active,
        topic_two_inactive,
        namespace_one,
        namespace_two,
        source_one_active,
        source_two_inactive
    ):
        dict_one = IntrospectorTopic(
            topic_one_active,
            kafka_topics=base_command._kafka_topics,
            topics_to_range_map=base_command._topics_with_messages_to_range_map
        ).to_ordered_dict()
        dict_two = IntrospectorTopic(
            topic_two_inactive,
            kafka_topics=base_command._kafka_topics,
            topics_to_range_map=base_command._topics_with_messages_to_range_map
        ).to_ordered_dict()
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=dict_one,
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True
        )
        self._assert_topic_equals_topic_dict(
            topic=topic_two_inactive,
            topic_dict=dict_two,
            namespace_name=namespace_two,
            source_name=source_two_inactive,
            is_active=False
        )

    def test_introspector_topic_no_kafka_info(
        self,
        topic_one_active,
        namespace_one,
        source_one_active,
    ):
        topic_dict = IntrospectorTopic(
            topic_one_active
        ).to_ordered_dict()
        self._assert_topic_equals_topic_dict(
            topic=topic_one_active,
            topic_dict=topic_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            is_active=True,
            include_kafka_info=False
        )

    def test_introspector_source(
        self,
        base_command,
        source_one_active,
        source_two_inactive,
        topic_one_active,
        topic_two_inactive,
        namespace_one,
        namespace_two
    ):
        source_one_obj = topic_one_active.source
        source_two_obj = topic_two_inactive.source
        source_one_dict = IntrospectorSource(
            source_one_obj,
            active_sources=base_command.active_sources
        ).to_ordered_dict()
        source_two_dict = IntrospectorSource(
            source_two_obj,
            active_sources=base_command.active_sources
        ).to_ordered_dict()
        self._assert_source_equals_source_dict(
            source=source_one_obj,
            source_dict=source_one_dict,
            namespace_name=namespace_one,
            source_name=source_one_active,
            active_topic_count=1
        )
        self._assert_source_equals_source_dict(
            source=source_two_obj,
            source_dict=source_two_dict,
            namespace_name=namespace_two,
            source_name=source_two_inactive,
            active_topic_count=0
        )

    def test_introspector_source_no_active_sources(
        self,
        source_one_active,
        topic_one_active,
        namespace_one
    ):
        source_obj = topic_one_active.source
        source_dict = IntrospectorSource(source_obj).to_ordered_dict()
        self._assert_source_equals_source_dict(
            source=source_obj,
            source_dict=source_dict,
            namespace_name=namespace_one,
            source_name=source_one_active
        )

    def test_introspector_namespace(
        self,
        base_command,
        topic_one_active,
        topic_two_active,
        namespace_one,
        namespace_two
    ):
        namespace_one_obj = topic_one_active.source.namespace
        namespace_two_obj = topic_two_active.source.namespace
        namespace_one_dict = IntrospectorNamespace(
            namespace_one_obj,
            active_namespaces=base_command.active_namespaces
        ).to_ordered_dict()
        namespace_two_dict = IntrospectorNamespace(
            namespace_two_obj,
            active_namespaces=base_command.active_namespaces
        ).to_ordered_dict()
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_one_obj,
            namespace_dict=namespace_one_dict,
            namespace_name=namespace_one
        )
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_two_obj,
            namespace_dict=namespace_two_dict,
            namespace_name=namespace_two
        )

    def test_introspector_namespace_no_active_namespaces(
        self,
        topic_one_active,
        namespace_one
    ):
        namespace_obj = topic_one_active.source.namespace
        namespace_dict = IntrospectorNamespace(
            namespace_obj
        ).to_ordered_dict()
        self._assert_namespace_equals_namespace_dict(
            namespace=namespace_obj,
            namespace_dict=namespace_dict,
            namespace_name=namespace_one,
            assert_active_counts=False
        )
