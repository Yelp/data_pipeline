# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import glob


def get_file_paths_from_glob_patterns(glob_patterns):
    """ Return a set of files matching the given list of glob patterns
     (for example ["./test.sql", "./other_tables/*.sql"])
    """
    file_paths = set()
    for glob_pattern in glob_patterns:
        file_paths |= set(glob.glob(glob_pattern))
    return file_paths
