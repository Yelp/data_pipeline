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

from data_pipeline.tools._glob_util import get_file_paths_from_glob_patterns


def test_get_file_paths_from_glob_patterns():
    with mock.patch('data_pipeline.tools._glob_util.glob') as mock_glob:
        mock_glob.glob = mock.Mock(return_value=['test'])
        paths = get_file_paths_from_glob_patterns(['*.sql', 'some/dir/*.avsc'])
        assert paths == {'test'}
        assert mock_glob.glob.mock_calls == [
            mock.call('*.sql'),
            mock.call('some/dir/*.avsc'),
        ]
