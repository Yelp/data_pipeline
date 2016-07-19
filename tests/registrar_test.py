# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.client import Client
from data_pipeline.expected_frequency import ExpectedFrequency


class ClientTester(Client):
    @property
    def client_type(self):
        return 'tester'


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
