# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import pytest

from data_pipeline.client import Client
from data_pipeline.expected_frequency import ExpectedFrequency
from tests.helpers.mock_utils import attach_spy_on_func


class ClientTester(Client):
    @property
    def client_type(self):
        return 'consumer'


@pytest.mark.usefixtures("configure_teams")
class TestRegistration(object):
    @property
    def client_name(self):
        return 'test_client'

    @property
    def team_name(self):
        return 'bam'

    @property
    def expected_frequency_seconds(self):
        return ExpectedFrequency.constantly

    def _build_client(self, **override_kwargs):
        args = {
            'client_name': self.client_name,
            'team_name': self.team_name,
            'expected_frequency_seconds': self.expected_frequency_seconds,
            'monitoring_enabled': False
        }
        args.update(override_kwargs)
        return ClientTester(**args)

    def test_registration_message_schema(self, schematizer_client):
        client = self._build_client()
        actual_schema = client.registrar.registration_schema()
        schema_id = client.registrar.registration_schema().schema_id
        # _registration_schema() returns the actual json read from the file
        expected_schema_json = client.registrar._registration_schema()
        actual_schema_json = actual_schema.schema_json
        assert expected_schema_json == actual_schema_json
        assert schema_id > 0

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

    def test_periodic_wake_calls(self):
        """
        Test that calling start() periodically publishes messages at the expected rate
        until stop() is called.
        """
        client = self._build_client()
        with attach_spy_on_func(
            client.registrar,
            'publish_registration_messages'
        ) as func_spy:
            client.registrar.threshold = 1
            client.registrar.start()
            time.sleep(3.5)
            client.registrar.stop()
            time.sleep(.5)
            assert func_spy.call_count == 4
