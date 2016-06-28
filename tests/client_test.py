# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

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


class TestClientRegistration(TestClient):
    @pytest.fixture
    def timestamp(self):
        """Returns a sample timestamp coverted to long format"""
        ts_str = "2016-01-01T19:10:26"
        ts = datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')
        ts_long = long((ts - datetime.utcfromtimestamp(0)).total_seconds())
        return ts_long

    def test_register_schema_ids(self, timestamp):
        client = self._build_client()
        id_list = [1, 4, 11]
        client.registrar.register_schema_ids(id_list, timestamp)
        schema_map = client.registrar.schema_time_map
        for schema_id in id_list:
            assert timestamp == schema_map[schema_id]
        # TODO([DATAPIPE-1192|mkohli]): Assert that registration message was sent

    def test_register_active_schemas(self, timestamp):
        """
        Tests 3 cases for register_active_schemas
        1. Regular update of schema ID with later date
        2. Updating with past date should have no effect
        3. Updating schema ID which Client has not seen yet creates new entry

        """
        timestamp_after = timestamp + 500
        timestamp_before = timestamp - 500

        client = self._build_client()
        id_list = [1, 4]
        client.registrar.register_schema_ids(id_list, timestamp)
        client.registrar.register_active_schema(1, timestamp_after)
        client.registrar.register_active_schema(4, timestamp_before)
        client.registrar.register_active_schema(11, timestamp_before)
        schema_map = client.registrar.schema_time_map

        assert schema_map[1] == timestamp_after
        assert schema_map[4] == timestamp
        assert schema_map[11] == timestamp_before
        # TODO([DATAPIPE-1192|mkohli]): Assert that registration message was sent
