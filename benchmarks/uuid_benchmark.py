from __future__ import absolute_import

from cached_property import cached_property
from data_pipeline._fast_uuid import FastUUID
from benchmarks.benchmarks import Benchmarks

import uuid


class UUIDBenchmark(object):
    @cached_property
    def fast_uuid(self):
        return FastUUID()

    def benchmark_fastuuid_uuid1(self):
        self.fast_uuid.uuid1()

    def benchmark_fastuuid_uuid4(self):
        self.fast_uuid.uuid4()

    def benchmark_python_uuid1(self):
        uuid.uuid1().bytes

    def benchmark_python_uuid3(self):
        uuid.uuid4().bytes


if __name__ == '__main__':
    Benchmarks().execute_benchmark_class(UUIDBenchmark)
