from __future__ import absolute_import

from cached_property import cached_property
from contextlib import contextmanager
from data_pipeline.producer import Producer
from data_pipeline.producer import TopicAndMessage
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from benchmarks.benchmarks import Benchmarks


class ProducerBenchmark(object):
    @property
    def message(self):
        return Message(10, bytes(10), MessageType.create)

    @property
    def topic_and_message(self):
        return TopicAndMessage(topic='my-topic', message=self.message)

    @contextmanager
    def yield_async_producer(self):
        with Producer(async=True, use_work_pool=False) as producer:
            yield producer

    @contextmanager
    def yield_sync_producer(self):
        with Producer(async=False, use_work_pool=False) as producer:
            yield producer

    @contextmanager
    def yield_sync_pooled_producer(self):
        with Producer(async=False, use_work_pool=True) as producer:
            yield producer

    @contextmanager
    def yield_async_pooled_producer(self):
        with Producer(async=True, use_work_pool=True) as producer:
            yield producer

    def benchmark_async_publish(self, async_producer):
        async_producer.publish(self.topic_and_message)

    def benchmark_sync_publish(self, sync_producer):
        sync_producer.publish(self.topic_and_message)

    def benchmark_sync_pooled_publish(self, sync_pooled_producer):
        sync_pooled_producer.publish(self.topic_and_message)

    def benchmark_async_pooled_publish(self, async_pooled_producer):
        async_pooled_producer.publish(self.topic_and_message)


if __name__ == '__main__':
    Benchmarks().execute_benchmark_class(ProducerBenchmark)
