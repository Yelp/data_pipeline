# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

from cached_property import cached_property

from benchmarks.benchmarks import Benchmarks
from data_pipeline.async_producer import AsyncProducer
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from data_pipeline.producer import Producer


class ProducerBenchmark(object):
    @cached_property
    def message(self):
        return Message(str('my-topic'), 10, bytes(10), MessageType.create)

    @contextmanager
    def yield_async_producer(self):
        with AsyncProducer(use_work_pool=False) as producer:
            yield producer

    @contextmanager
    def yield_sync_producer(self):
        with Producer(use_work_pool=False) as producer:
            yield producer

    @contextmanager
    def yield_sync_pooled_producer(self):
        with Producer(use_work_pool=True) as producer:
            yield producer

    @contextmanager
    def yield_async_pooled_producer(self):
        with AsyncProducer(use_work_pool=True) as producer:
            yield producer

    def benchmark_async_publish(self, async_producer):
        async_producer.publish(self.message)

    def benchmark_sync_publish(self, sync_producer):
        sync_producer.publish(self.message)

    def benchmark_sync_pooled_publish(self, sync_pooled_producer):
        sync_pooled_producer.publish(self.message)

    def benchmark_async_pooled_publish(self, async_pooled_producer):
        async_pooled_producer.publish(self.message)


if __name__ == '__main__':
    Benchmarks().execute_benchmark_class(ProducerBenchmark)
