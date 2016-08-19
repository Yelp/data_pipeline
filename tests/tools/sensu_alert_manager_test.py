# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime
from datetime import timedelta

import mock
import pytest
from dateutil.tz import tzutc

from data_pipeline.tools.sensu_alert_manager import SensuAlertManager


class TestSensuAlertManager(object):

    @pytest.fixture
    def sensu_alert_manager(self):
        basic_dict = {"check_every": 60, "ttl": "300s"}
        return SensuAlertManager(
            interval_in_seconds=30,
            service_name="test_service",
            result_dict=basic_dict,
            max_delay_seconds=120,
            disable=False
        )

    @pytest.yield_fixture
    def mocked_log_and_send(self, sensu_alert_manager):
        with mock.patch.object(
            sensu_alert_manager,
            'log_and_send_event',
            autospec=True
        ) as mocked_log_and_send:
            yield mocked_log_and_send

    def test_process_no_timestamp(self, sensu_alert_manager, mocked_log_and_send):
        sensu_alert_manager.process()
        assert mocked_log_and_send.call_count == 0

    def test_process_with_recent_timestamp(self, sensu_alert_manager, mocked_log_and_send):
        sensu_alert_manager.process(datetime.now(tzutc()))
        assert mocked_log_and_send.call_count == 1
        assert mocked_log_and_send.call_args[0][0]['output'] == \
            "test_service has caught up to real time"

    def test_process_with_old_timestamp(self, sensu_alert_manager, mocked_log_and_send):
        old_time = datetime.now(tzutc()) - timedelta(hours=24)
        sensu_alert_manager.process(old_time)
        assert mocked_log_and_send.call_count == 1
        assert "min behind real time" in mocked_log_and_send.call_args[0][0]['output']

    def test_toggling_disable_to_true(self, sensu_alert_manager, mocked_log_and_send):
        sensu_alert_manager.disable = True
        assert mocked_log_and_send.call_count == 1
        assert mocked_log_and_send.call_args[0][0]['output'] == \
            "disabling sensu alert for test_service"
