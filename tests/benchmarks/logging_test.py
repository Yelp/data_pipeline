# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline.config import get_config
from data_pipeline.helpers.log import debug_log
from tests.factories.base_factory import MessageFactory


@pytest.mark.usefixtures(
    "config_containers_connections"
)
@pytest.mark.benchmark
class TestBenchLogging(object):

    @pytest.fixture(
        params=[True, False],
        ids=['logger enabled', 'logger disabled']
    )
    def logger_enabled(self, request):
        return request.param

    @pytest.yield_fixture
    def patch_logger_enabled(self, logger_enabled):
        with mock.patch(
            'data_pipeline.config.logging.Logger'
            '.isEnabledFor',
            return_value=logger_enabled
        ):
            yield

    @pytest.fixture
    def message(self):
        return MessageFactory.create_message_with_payload_data()

    def test_debug_log(self, message, benchmark, patch_logger_enabled):
        @benchmark
        def log_command():
            debug_log(lambda: "Message buffered: {}".format(repr(message)))

    def test_logger(self, benchmark, message, patch_logger_enabled):
        @benchmark
        def log_command():
            get_config().logger.debug("Message buffered: {}".format(repr(message)))

    def test_repr_message(self, benchmark, message):
        @benchmark
        def log_command():
            repr(message)

    def test_pass(self, benchmark, message):
        @benchmark
        def log_command():
            pass
