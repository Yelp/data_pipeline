# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from collections import namedtuple

from data_pipeline.tools.introspector.list_command import ListCommand

class FakeParserError(Exception):
    pass

Args = namedtuple("Namespace", [
        "list_type",
        "namespace_filter",
        "source_filter",
        "sort_by",
        "descending_order",
        "verbosity"
    ]
)

class TestListCommand(object):

    @pytest.fixture
    def list_command(self):
        list_command = ListCommand("data_pipeline_introspector_list")
        list_command.log = mock.Mock()
        list_command.log.info = mock.Mock()
        list_command.log.debug = mock.Mock()
        list_command.log.warning = mock.Mock()
        # Need to have return_values since we won't mock simplejson if
        # we don't have to
        list_command.list_topics = mock.Mock(return_value={})
        list_command.list_sources = mock.Mock(return_value={})
        list_command.list_namespaces = mock.Mock(return_value={})
        return list_command

    @pytest.fixture
    def parser(self):
        parser = mock.Mock()
        parser.error = FakeParserError
        return parser

    @pytest.fixture
    def namespace_name(self):
        return "test_namespace"

    @pytest.fixture
    def source_name(self):
        return "test_source"

    @pytest.fixture
    def source_id(self):
        return "42"

    @pytest.fixture(params=["namespaces", "sources", "topics"])
    def list_type(self, request):
        return request.param

    @pytest.fixture(params=[
        {"namespace_filter": namespace_name("fake_self")},
        {"source_filter": source_name("fake_self")},
        {}
    ], ids=["namespace_name", "source_name", "nothing"])
    def field_name_dict(self, request):
        return request.param

    def _create_fake_args(
        self,
        list_type,
        namespace_filter=None,
        source_filter=None,
        sort_by=None,
        descending_order=False
    ):
        return Args(
            list_type=list_type,
            namespace_filter=namespace_filter,
            source_filter=source_filter,
            sort_by=sort_by,
            descending_order=descending_order,
            verbosity=0
        )

    def _assert_list_command_values(
        self,
        list_command,
        list_type,
        namespace_filter=None,
        source_id_filter=None,
        source_name_filter=None,
        sort_by=None,
        descending_order=False
    ):
        assert list_command.list_type == list_type
        assert list_command.namespace_filter == namespace_filter
        assert list_command.source_id_filter == source_id_filter
        assert list_command.source_name_filter == source_name_filter
        assert list_command.sort_by == sort_by
        assert list_command.descending_order == descending_order


    def test_bad_sort_by_fields(self, list_command, parser, list_type, source_id):
        # Using source id since it guarantees we won't get an error for any list_type
        # (Only warnings, if anything)
        args = self._create_fake_args(
            list_type=list_type,
            source_filter=source_id,
            sort_by="bad_field"
        )
        with pytest.raises(FakeParserError) as e:
            list_command.process_args(args, parser)
        assert e.value.args
        assert "You can not sort_by by {} for list type {}".format(
            "bad_field", list_type
        ) in e.value.args[0]

    def test_namespaces_good(self, list_command, parser):
        args = self._create_fake_args(
            list_type="namespaces",
            sort_by="name",
            descending_order=True
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="namespaces",
            sort_by="name",
            descending_order=True
        )
        assert list_command.log.warning.call_count == 0

    def test_namespaces_warning(self, list_command, parser, namespace_name, source_name):
        args = self._create_fake_args(
            list_type="namespaces",
            namespace_filter=namespace_name,
            source_filter=source_name
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="namespaces",
            namespace_filter=namespace_name,
            source_name_filter=source_name
        )
        list_command.log.warning.has_calls([
            mock.call("Will not use --namespace-filter to filter namespaces"),
            mock.call("Will not use --source-filter to filter namespaces")
        ])

    def test_sources_good_name(self, list_command, parser, namespace_name):
        args = self._create_fake_args(
            list_type="sources",
            namespace_filter=namespace_name,
            sort_by="name"
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="sources",
            namespace_filter=namespace_name,
            sort_by="name"
        )
        assert list_command.log.warning.call_count == 0

    def test_sources_source_name_warning(self, list_command, parser, namespace_name, source_name):
        args = self._create_fake_args(
            list_type="sources",
            namespace_filter=namespace_name,
            source_filter=source_name,
            sort_by="name"
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="sources",
            namespace_filter=namespace_name,
            source_name_filter=source_name,
            sort_by="name"
        )
        list_command.log.warning.has_calls([
            mock.call("Will not use --source-filter to filter sources")
        ])

    def test_sources_good_no_filter(self, list_command, parser):
        args = self._create_fake_args(
            list_type="sources"
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="sources"
        )
        assert list_command.log.warning.call_count == 0

    def test_topics_good_id(self, list_command, parser, source_id):
        args = self._create_fake_args(
            list_type="topics",
            source_filter=source_id,
            sort_by="primary_keys",
            descending_order=True
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="topics",
            source_id_filter=int(source_id),
            sort_by="primary_keys",
            descending_order=True
        )
        assert list_command.log.warning.call_count == 0

    def test_topics_good_names(self, list_command, parser, source_name, namespace_name):
        args = self._create_fake_args(
            list_type="topics",
            source_filter=source_name,
            namespace_filter=namespace_name
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="topics",
            source_name_filter=source_name,
            namespace_filter=namespace_name
        )
        assert list_command.log.warning.call_count == 0

    def test_topics_only_one_name_or_nothing(self, list_command, parser, field_name_dict):
        args = self._create_fake_args(
            list_type="topics",
            **field_name_dict
        )
        with pytest.raises(FakeParserError) as e:
            list_command.process_args(args, parser)
        assert e.value.args
        assert "Must provide topic filters of" in e.value.args[0]

    def test_topics_id_override(self, list_command, parser, source_id, namespace_name):
        args = self._create_fake_args(
            list_type="topics",
            source_filter=source_id,
            namespace_filter=namespace_name,
            sort_by="topic_id"
        )
        list_command.process_args(args, parser)
        self._assert_list_command_values(
            list_command,
            list_type="topics",
            source_id_filter=int(source_id),
            namespace_filter=namespace_name,
            sort_by="topic_id"
        )
        list_command.log.warning.has_calls([
            mock.call("Overring all other filters with --source-filter"),
        ])

    def test_run_namespaces(self, list_command, parser, namespace_name):
        args = self._create_fake_args(
            list_type="namespaces"
        )
        list_command.run(args, parser)
        assert list_command.list_topics.call_count == 0
        assert list_command.list_sources.call_count == 0
        list_command.list_namespaces.assert_called_once_with(
            sort_by=None,
            descending_order=False
        )

    def test_run_sources(self, list_command, parser, namespace_name):
        args = self._create_fake_args(
            list_type="sources",
            namespace_filter=namespace_name
        )
        list_command.run(args, parser)
        assert list_command.list_topics.call_count == 0
        assert list_command.list_namespaces.call_count == 0
        list_command.list_sources.assert_called_once_with(
            namespace_name=namespace_name,
            sort_by=None,
            descending_order=False
        )

    def test_run_topics(self, list_command, parser, source_id):
        args = self._create_fake_args(
            list_type="topics",
            source_filter=source_id
        )
        list_command.run(args, parser)
        assert list_command.list_sources.call_count == 0
        assert list_command.list_namespaces.call_count == 0
        list_command.list_topics.assert_called_once_with(
            source_id=int(source_id),
            namespace_name=None,
            source_name=None,
            sort_by=None,
            descending_order=False
        )
