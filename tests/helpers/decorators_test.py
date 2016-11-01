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

import tempfile

import pytest

from data_pipeline.helpers.decorators import memoized


def fibonacci(n):
    if n <= 0:
        return 0
    if n == 1:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)


@memoized
def fast_fibonacci(n):
    if n <= 0:
        return 0
    if n == 1:
        return 1
    else:
        return fast_fibonacci(n - 1) + fast_fibonacci(n - 2)


@memoized
def identity(x):
    return x


class TestMemoized(object):
    """Ensure memoization decorator behaves per its specification"""

    def test_basic(self):
        """Basic correctness tests"""
        assert identity((1,)) == (1,)
        assert fibonacci(1) == fast_fibonacci(1)
        assert fibonacci(2) == fast_fibonacci(2)
        assert fibonacci(3) == fast_fibonacci(3)
        assert fibonacci(10) == fast_fibonacci(10)

    def test_unhashable_args(self):
        """The memoization decorator should even work with
        common unhashable arguments ..."""
        assert identity([1]) == [1]
        assert identity(set([1])) == set([1])
        assert identity({'a': 1}) == {'a': 1}

    def test_uncacheable_args(self):
        """... but might not work with all unhashable objects."""
        f = tempfile.NamedTemporaryFile()
        with pytest.raises(TypeError):
            identity(f)

    def test_performance(self):
        """Ensure that the memoization decorator actually saves
        function calls"""

        @memoized
        def my_identity(x, sheep=False):
            my_identity.num_calls += 1
            if sheep:
                return "sheep"
            else:
                return x

        my_identity.num_calls = 0

        assert my_identity(1) == 1
        assert my_identity(1) == 1
        assert my_identity(1) == 1
        assert my_identity(2) == 2
        assert my_identity(2) == 2
        assert my_identity(2) == 2

        assert my_identity.num_calls == 2

        # Ensure kwargs work
        assert my_identity(1, sheep=True) == "sheep"
        assert my_identity(1, sheep=True) == "sheep"
        assert my_identity(2, sheep=True) == "sheep"
        assert my_identity(2, sheep=True) == "sheep"

        assert my_identity.num_calls == 4
