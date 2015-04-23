"""
Tests that runnable documentation is up-to-date and executable.
"""
import doctest
import os


class TestDocs(object):

    def test_usage(self):
        example_file = os.path.join(os.path.dirname(__file__), '../USAGE.rst')
        failure_count, test_count = doctest.testfile(example_file, False)
        assert failure_count == 0
        assert test_count > 0
