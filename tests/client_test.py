# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

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
    def schema_last_used_timestamp(self):
        """Returns a sample timestamp coverted to long format"""
        return long(time.time())

    def test_register_tracked_schema_ids(self):
        client = self._build_client()
        schema_id_list = [1, 4, 11]
        client.registrar.register_tracked_schema_ids(schema_id_list)
        schema_map = client.registrar.schema_to_last_seen_time_map
        for schema_id in schema_id_list:
            assert schema_map[schema_id] is None
        # TODO([DATAPIPE-1192|mkohli]): Assert that registration message was sent

    def test_update_first_time_used_timestamp(self, schema_last_used_timestamp):
        """
        Test that updating a schema ID in Client that has not been used before
        creates a new internal entry.
        """
        client = self._build_client()
        client.registrar.update_schema_last_used_timestamp(11, schema_last_used_timestamp)
        schema_map = client.registrar.schema_to_last_seen_time_map
        assert schema_map.get(11) == schema_last_used_timestamp

    def test_update_to_later_used_timestamp(self, schema_last_used_timestamp):
        """
        Test that updating a schema ID in Client to a later timestamp than it had
        been used last successfully updates its internal entry.
        """
        client = self._build_client()
        timestamp_after = schema_last_used_timestamp + 500
        timestamp_before = schema_last_used_timestamp - 500
        schema_id_list = [1, 4]
        client.registrar.register_tracked_schema_ids(schema_id_list)
        client.registrar.update_schema_last_used_timestamp(1, timestamp_before)
        client.registrar.update_schema_last_used_timestamp(1, timestamp_after)
        schema_map = client.registrar.schema_to_last_seen_time_map
        assert schema_map.get(1) == timestamp_after

    def test_update_with_earlier_used_timestamp(self, schema_last_used_timestamp):
        """
        Test that updating a schema ID in Client to an earlier timestamp than it had
        been used last does not update its internal entry.
        """
        client = self._build_client()
        timestamp_after = schema_last_used_timestamp + 500
        timestamp_before = schema_last_used_timestamp - 500
        schema_id_list = [1, 4]
        client.registrar.register_tracked_schema_ids(schema_id_list)
        client.registrar.update_schema_last_used_timestamp(4, timestamp_after)
        client.registrar.update_schema_last_used_timestamp(4, timestamp_before)
        schema_map = client.registrar.schema_to_last_seen_time_map
        assert schema_map.get(4) == timestamp_after
