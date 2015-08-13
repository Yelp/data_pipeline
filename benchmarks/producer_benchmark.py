# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

from cached_property import cached_property

from benchmarks.benchmarks import Benchmarks
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from data_pipeline.producer import Producer


class ProducerBenchmark(object):
    @cached_property
    def message(self):
        return CreateMessage(str('my-topic'), 10, payload=bytes(10))

    @contextmanager
    def yield_sync_producer(self):
        with self._create_producer(use_work_pool=False) as producer:
            yield producer

    @contextmanager
    def yield_sync_pooled_producer(self):
        with self._create_producer(use_work_pool=True) as producer:
            yield producer

    def benchmark_sync_publish(self, sync_producer):
        sync_producer.publish(self.message)

    def benchmark_sync_pooled_publish(self, sync_pooled_producer):
        sync_pooled_producer.publish(self.message)

    def _create_producer(self, **override_kwargs):
        kwargs = dict(
            producer_name='dp_benchmark',
            team_name='bam',
            expected_frequency=ExpectedFrequency.constantly
        )
        kwargs.update(override_kwargs)
        return Producer(**kwargs)


if __name__ == '__main__':
    Benchmarks().execute_benchmark_class(ProducerBenchmark)
