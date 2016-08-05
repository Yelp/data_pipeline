# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline.client import Client
from data_pipeline.expected_frequency import ExpectedFrequency


class ClientTester(Client):
    @property
    def client_type(self):
        return 'tester'


@pytest.mark.usefixtures('configure_teams')
class TestClient(object):
    @property
    def client_name(self):
        return 'test_client'

    @property
    def team_name(self):
        return 'bam'

    @property
    def expected_frequency_seconds(self):
        return 0

    def _build_client(self, **override_kwargs):
        args = dict(
            client_name=self.client_name,
            team_name=self.team_name,
            expected_frequency_seconds=self.expected_frequency_seconds,
            monitoring_enabled=False
        )
        args.update(override_kwargs)
        return ClientTester(**args)

    def test_default_client_is_valid(self):
        self._assert_valid(self._build_client())

    def test_string_client_name_is_valid(self):
        name = str("test_client")
        assert self._build_client(client_name=name).client_name == name

    def test_non_string_client_name(self):
        self._assert_invalid(client_name=1)

    def test_empty_client_name(self):
        self._assert_invalid(client_name='')

    def test_invalid_team_name(self):
        self._assert_invalid(team_name='bogus_team')

    def test_negative_expected_frequency_seconds(self):
        self._assert_invalid(expected_frequency_seconds=-1)

    def test_expected_frequency_seconds_constant_is_valid(self):
        client = self._build_client(
            expected_frequency_seconds=ExpectedFrequency.constantly
        )
        assert client.expected_frequency_seconds == 0

    def _assert_invalid(self, **client_kwargs):
        with pytest.raises(ValueError):
            self._build_client(**client_kwargs)

    def _assert_valid(self, client):
        assert client.client_name == self.client_name
        assert client.team_name == self.team_name
        assert client.expected_frequency_seconds == self.expected_frequency_seconds

    @pytest.mark.parametrize("method, skipped_method, kwargs", [
        ('record_message', '_get_record', {'message': None}),
        ('close', 'flush_buffered_info', {}),
    ])
    def test_method_call_with_disabled_monitoring(self, method, skipped_method, kwargs):
        client = self._build_client(
            expected_frequency_seconds=ExpectedFrequency.constantly
        )
        with mock.patch.object(client.monitor, skipped_method) as uncalled_method:
            getattr(client.monitor, method)(**kwargs)
            assert uncalled_method.called == 0
