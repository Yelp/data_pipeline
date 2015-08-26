# -*- coding: utf-8 -*-
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
