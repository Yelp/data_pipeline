# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import pytest

from data_pipeline.registrar import Registrar
from tests.helpers.mock_utils import attach_spy_on_func


@pytest.mark.usefixtures("configure_teams")
class TestRegistration(object):

    @pytest.fixture
    def registrar(self):
        return Registrar(
            team_name='bam',
            client_name='test_client',
            client_type='producer',
            expected_frequency_seconds=0,
            threshold=1
        )

    def test_registration_message_schema(self, schematizer_client, registrar):
        actual_schema = registrar.registration_schema
        schema_id = registrar.registration_schema.schema_id
        # _registration_schema returns the actual json read from the file
        expected_schema_json = registrar._registration_schema
        actual_schema_json = actual_schema.schema_json
        assert expected_schema_json == actual_schema_json
        assert schema_id > 0

    @pytest.fixture
    def schema_last_used_timestamp(self):
        """Returns a sample timestamp coverted to long format"""
        return long(time.time())

    def test_register_tracked_schema_ids(self, registrar):
        schema_id_list = [1, 4, 11]
        registrar.register_tracked_schema_ids(schema_id_list)
        schema_map = registrar.schema_to_last_seen_time_map
        expected = {1: None, 4: None, 11: None}
        assert schema_map == expected

    def test_update_first_time_used_timestamp(self, schema_last_used_timestamp, registrar):
        """
        Test that updating a schema ID in Client that has not been used before
        creates a new internal entry.
        """
        registrar.update_schema_last_used_timestamp(11, schema_last_used_timestamp)
        schema_map = registrar.schema_to_last_seen_time_map
        assert schema_map[11] == schema_last_used_timestamp

    def test_update_to_later_used_timestamp(self, schema_last_used_timestamp, registrar):
        """
        Test that updating a schema ID in Client to a later timestamp than it had
        been used last successfully updates its internal entry.
        """
        timestamp_after = schema_last_used_timestamp + 500
        timestamp_before = schema_last_used_timestamp - 500
        schema_id_list = [1, 4]
        registrar.register_tracked_schema_ids(schema_id_list)
        registrar.update_schema_last_used_timestamp(1, timestamp_before)
        registrar.update_schema_last_used_timestamp(1, timestamp_after)
        schema_map = registrar.schema_to_last_seen_time_map
        assert schema_map[1] == timestamp_after

    def test_update_with_earlier_used_timestamp(self, schema_last_used_timestamp, registrar):
        """
        Test that updating a schema ID in Client to an earlier timestamp than it had
        been used last does not update its internal entry.
        """
        timestamp_after = schema_last_used_timestamp + 500
        timestamp_before = schema_last_used_timestamp - 500
        schema_id_list = [1, 4]
        registrar.register_tracked_schema_ids(schema_id_list)
        registrar.update_schema_last_used_timestamp(4, timestamp_after)
        registrar.update_schema_last_used_timestamp(4, timestamp_before)
        schema_map = registrar.schema_to_last_seen_time_map
        assert schema_map[4] == timestamp_after

    def test_periodic_wake_calls(self, registrar):
        """
        Test that calling start() periodically publishes messages at the expected rate
        until stop() is called.
        """
        with attach_spy_on_func(
            registrar,
            'publish_registration_messages'
        ) as func_spy:
            registrar.threshold = 1
            registrar.start()
            time.sleep(3.5)
            registrar.stop()
            time.sleep(.5)
            # One call to publish_registration_messages happens on stop()
            assert func_spy.call_count == 4
