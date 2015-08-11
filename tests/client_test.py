# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.client import Client
from data_pipeline.expected_frequency import ExpectedFrequency


@pytest.mark.usefixtures('configure_teams')
class TestClient(object):
    def _build_client(self, **override_kwargs):
        args = dict(
            client_name='test_client',
            team_name='bam',
            expected_frequency=0,
            monitoring_enabled=False
        )
        args.update(override_kwargs)
        return Client(**args)

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

    def test_negative_expected_frequency(self):
        self._assert_invalid(expected_frequency=-1)

    def test_expected_frequency_constant_is_valid(self):
        client = self._build_client(expected_frequency=ExpectedFrequency.constantly)
        assert client.expected_frequency == 0

    def _assert_invalid(self, **client_kwargs):
        with pytest.raises(ValueError):
            self._build_client(**client_kwargs)

    def _assert_valid(self, client):
        assert client.client_name == 'test_client'
        assert client.team_name == 'bam'
        assert client.expected_frequency == 0
