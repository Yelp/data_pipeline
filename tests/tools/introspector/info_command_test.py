# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from collections import namedtuple

from data_pipeline.tools.introspector.info_command import InfoCommand

class FakeParserError(Exception):
    pass

Args = namedtuple("Namespace", [
        "info_type",
        "identifier",
        "verbosity"
    ]
)

class TestInfoCommand(object):

    @pytest.fixture
    def info_command(self):
        info_command = InfoCommand("data_pipeline_introspector_info")
        info_command.log = mock.Mock()
        info_command.log.info = mock.Mock()
        info_command.log.debug = mock.Mock()
        info_command.log.warning = mock.Mock()
        # Need to have return_values since we won't mock simplejson if
        # we don't have to
        info_command.info_topic = mock.Mock(return_value={})
        info_command.info_source = mock.Mock(return_value={})
        info_command.info_namespace = mock.Mock(return_value={})
        return info_command

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
    def compacted_names(self, source_name, namespace_name):
        return "|".join([source_name, namespace_name])

    @pytest.fixture(params=["too_few_names", "too|many|names"])
    def bad_name(self, request):
        return request.param

    @pytest.fixture
    def source_id(self):
        return "42"

    @pytest.fixture
    def topic_name(self):
        return "test_topic"

    def _create_fake_args(
        self,
        info_type,
        identifier
    ):
        return Args(
            info_type=info_type,
            identifier=identifier,
            verbosity=0
        )

    def _assert_info_command_values(
        self,
        info_command,
        info_type,
        identifier,
        namespace_name=None,
        source_id=None,
        source_name=None
    ):
        assert info_command.info_type == info_type
        assert info_command.identifier == identifier
        if info_type == "source":
            assert info_command.namespace_name == namespace_name
            assert info_command.source_id == source_id
            assert info_command.source_name == source_name

    def test_topic(self, info_command, parser, topic_name):
        args = self._create_fake_args(
            info_type="topic",
            identifier=topic_name
        )
        info_command.run(args, parser)
        self._assert_info_command_values(
            info_command,
            info_type="topic",
            identifier=topic_name
        )
        info_command.info_topic.assert_called_once_with(topic_name)
        assert info_command.info_source.call_count == 0
        assert info_command.info_namespace.call_count == 0

    def test_source_name(
        self,
        info_command,
        parser,
        source_name,
        namespace_name,
        compacted_names
    ):
        args = self._create_fake_args(
            info_type="source",
            identifier=compacted_names
        )
        info_command.run(args, parser)
        self._assert_info_command_values(
            info_command,
            info_type="source",
            identifier=compacted_names,
            namespace_name=namespace_name,
            source_name=source_name
        )
        info_command.info_source.assert_called_once_with(
            source_id=None,
            source_name=source_name,
            namespace_name=namespace_name
        )
        assert info_command.info_topic.call_count == 0
        assert info_command.info_namespace.call_count == 0

    def test_source_id(
        self,
        info_command,
        parser,
        source_id
    ):
        args = self._create_fake_args(
            info_type="source",
            identifier=source_id
        )
        info_command.run(args, parser)
        self._assert_info_command_values(
            info_command,
            info_type="source",
            identifier=source_id,
            source_id=int(source_id)
        )
        info_command.info_source.assert_called_once_with(
            source_id=int(source_id),
            source_name=None,
            namespace_name=None
        )
        assert info_command.info_topic.call_count == 0
        assert info_command.info_namespace.call_count == 0

    def test_namespace(
        self,
        info_command,
        parser,
        namespace_name
    ):
        args = self._create_fake_args(
            info_type="namespace",
            identifier=namespace_name
        )
        info_command.run(args, parser)
        self._assert_info_command_values(
            info_command,
            info_type="namespace",
            identifier=namespace_name,
            namespace_name=namespace_name
        )
        info_command.info_namespace.assert_called_once_with(
            namespace_name
        )
        assert info_command.info_topic.call_count == 0
        assert info_command.info_source.call_count == 0

    def test_bad_source_names(
        self,
        info_command,
        parser,
        bad_name
    ):
        args = self._create_fake_args(
            info_type="source",
            identifier=bad_name
        )
        with pytest.raises(FakeParserError) as e:
            info_command.run(args, parser)
        assert e.value.args
        assert "Source identifier must be" in e.value.args[0]
