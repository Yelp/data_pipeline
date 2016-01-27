# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline.tools.refresh_job import FullRefreshJob
from data_pipeline.tools.refresh_job import UndefinedPriorityException


class TestFullRefreshJob(object):

    @pytest.yield_fixture
    def mock_config(self):
        with mock.patch(
            'data_pipeline.tools.refresh_job.load_default_config'
        ), mock.patch(
            'data_pipeline.tools.refresh_job.get_schematizer'
        ):
            yield

    @pytest.yield_fixture
    def refresh_job(self, mock_config):
        refresh_job = FullRefreshJob()
        with mock.patch.object(
            refresh_job,
            'schematizer',
            new_callable=mock.PropertyMock
        ):
            yield refresh_job

    def test_invalid_priority(self, refresh_job):
        refresh_job.process_commandline_options(['--priority=INVALID'])
        with pytest.raises(UndefinedPriorityException) as e:
            refresh_job.validate_priority()
        assert e.value.message == ("Priority is not one of: LOW, MEDIUM,"
                                   " HIGH, MAX")

    def test_valid_priority(self, refresh_job):
        refresh_job.process_commandline_options(['--priority=MEDIUM'])
        refresh_job.validate_priority()

    def test_run_invalid_batch(self, refresh_job):
        refresh_job.process_commandline_options(
            [
                '--priority=MEDIUM',
                '--batch-size=0'
            ]
        )
        with pytest.raises(ValueError) as e:
            refresh_job.run()
        assert e.value.message == "--batch-size option must be greater than 0."

    def test_valid_run(self, refresh_job):
        refresh_job.process_commandline_options(
            [
                '--source-id=0',
                '--batch-size=250',
                '--priority=MAX',
                '--offset=0'
            ]
        )
        refresh_job.run()
        refresh_job.schematizer.create_refresh.assert_called_once_with(
            avg_rows_per_second_cap=None,
            source_id=0,
            batch_size=250,
            filter_condition=None,
            priority='MAX',
            offset=0
        )
