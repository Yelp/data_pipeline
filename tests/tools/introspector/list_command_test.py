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

from data_pipeline.tools.introspector.list_command.namespaces import NamespacesListCommand
from data_pipeline.tools.introspector.list_command.sources import SourcesListCommand
from data_pipeline.tools.introspector.list_command.topics import TopicsListCommand
from tests.tools.introspector.base_test import FakeParserError
from tests.tools.introspector.base_test import TestIntrospectorBase

NamespacesArgs = namedtuple(
    "Namespace", [
        "sort_by",
        "descending_order",
        "active_namespaces",
        "verbosity"
    ]
)

SourcesArgs = namedtuple(
    "Namespace", [
        "namespace",
        "sort_by",
        "descending_order",
        "active_sources",
        "verbosity"
    ]
)

TopicsArgs = namedtuple(
    "Namespace", [
        "source_id",
        "source_name",
        "namespace",
        "sort_by",
        "descending_order",
        "verbosity"
    ]
)

CommandArgsPair = namedtuple("Namespace", ["command", "args"])


class TestListCommand(TestIntrospectorBase):

    def _create_list_command(self, command):
        list_command = command("data_pipeline_introspector_list_topic")
        list_command.log = mock.Mock()
        list_command.log.info = mock.Mock()
        list_command.log.debug = mock.Mock()
        list_command.log.warning = mock.Mock()
        return list_command

    @pytest.fixture
    def topics_list_command(self):
        return self._create_list_command(TopicsListCommand)

    @pytest.fixture
    def sources_list_command(self):
        return self._create_list_command(SourcesListCommand)

    @pytest.fixture
    def namespaces_list_command(self):
        return self._create_list_command(NamespacesListCommand)

    @pytest.fixture
    def bad_topic_args(self, namespace_one, source_one_active):
        return TopicsArgs(
            source_id=None,
            source_name=source_one_active,
            namespace=namespace_one,
            sort_by="bad_field",
            descending_order=False,
            verbosity=0
        )

    @pytest.fixture
    def good_topic_args(self, namespace_one, source_one_active):
        return TopicsArgs(
            source_id=None,
            source_name=source_one_active,
            namespace=namespace_one,
            sort_by="message_count",
            descending_order=False,
            verbosity=0
        )

    @pytest.fixture
    def bad_source_args(self, namespace_one):
        return SourcesArgs(
            namespace=namespace_one,
            sort_by="bad_field",
            descending_order=False,
            active_sources=True,
            verbosity=0
        )

    @pytest.fixture(
        params=[True, False],
        ids=['with_active_sources', 'without_active_sources']
    )
    def good_source_args(self, request, namespace_one):
        return SourcesArgs(
            namespace=namespace_one,
            sort_by="name",
            descending_order=False,
            active_sources=request.param,
            verbosity=0
        )

    @pytest.fixture
    def bad_namespace_args(self):
        return NamespacesArgs(
            sort_by="bad_field",
            descending_order=False,
            active_namespaces=True,
            verbosity=0
        )

    @pytest.fixture(
        params=[True, False],
        ids=['with_active_namespaces', 'without_active_namespaces']
    )
    def good_namespace_args(self, request):
        return NamespacesArgs(
            sort_by="name",
            descending_order=False,
            active_namespaces=request.param,
            verbosity=0
        )

    def _assert_bad_fields(self, list_command, args, parser, list_type):
        with pytest.raises(FakeParserError) as e:
            list_command.run(args, parser)
        assert e.value.args
        assert "You can not sort_by by {} for list type {}".format(
            "bad_field", list_type
        ) in e.value.args[0]

    def _assert_good_fields(self, list_command, args, parser):
        list_command.run(args, parser)
        for field in args._fields:
            if field not in {"verbosity", "active_namespaces", "active_sources"}:
                assert getattr(list_command, field) == getattr(args, field)

    def test_bad_topics(
        self,
        bad_topic_args,
        topics_list_command,
        parser
    ):
        self._assert_bad_fields(
            topics_list_command,
            bad_topic_args,
            parser,
            'topics'
        )

    def test_good_topics(
        self,
        good_topic_args,
        topics_list_command,
        parser
    ):
        self._assert_good_fields(
            topics_list_command,
            good_topic_args,
            parser
        )

    def test_bad_sources(
        self,
        bad_source_args,
        sources_list_command,
        parser
    ):
        self._assert_bad_fields(
            sources_list_command,
            bad_source_args,
            parser,
            'sources'
        )

    def test_good_sources(
        self,
        good_source_args,
        sources_list_command,
        parser
    ):
        self._assert_good_fields(
            sources_list_command,
            good_source_args,
            parser
        )

    def test_bad_namespaces(
        self,
        bad_namespace_args,
        namespaces_list_command,
        parser
    ):
        self._assert_bad_fields(
            namespaces_list_command,
            bad_namespace_args,
            parser,
            'namespaces'
        )

    def test_good_namespaces(
        self,
        good_namespace_args,
        namespaces_list_command,
        parser
    ):
        self._assert_good_fields(
            namespaces_list_command,
            good_namespace_args,
            parser
        )
