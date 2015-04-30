from __future__ import absolute_import

import time
from kafka import create_message
from kafka.common import ProduceRequest
from cached_property import cached_property
from collections import defaultdict
from collections import namedtuple
from data_pipeline.config import get_kafka_client
from data_pipeline.client import Client
from data_pipeline.envelope import Envelope
from multiprocessing import Process, Queue, Event, Pool


EnvelopeAndMessage = namedtuple("EnvelopeAndMessage", ["envelope", "message"])


class Producer(Client):

    @cached_property
    def _kafka_producer(self):
        return _KafkaProducer(use_work_pool=self.use_work_pool)

    def __init__(self, use_work_pool=False):
        self.use_work_pool = use_work_pool

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False  # don't supress the exception, if there is one

    def publish(self, topic_and_message):
        self._kafka_producer.publish(topic_and_message)

    def flush(self):
        self._kafka_producer.flush_buffered_messages()

    def close(self):
        self._kafka_producer.close()


class AsyncProducer(Producer):
    """AsyncProducer wraps the Producer making many of the operations
    asynchronous.  The AsyncProducer uses a python multiprocessing queue
    to pass messages to a subprocess.  The subprocess is responsible for
    publishing messages to Kafka.
    """
    stop_marker = 0
    flush_marker = 1
    max_queue_size = 10000  # Double the number of messages to buffer

    def __init__(self, *args, **kwargs):
        super(AsyncProducer, self).__init__(*args, **kwargs)

        self.queue = Queue(self.max_queue_size)

        # This event is used to block the foreground process until the
        # background process is able to finish flushing, since flush should
        # be a syncronous operation
        self.flush_complete_event = Event()

        self.async_process = Process(target=self._consume_async_queue, args=(self.queue, self.flush_complete_event))
        self.async_process.start()

    def publish(self, topic_and_message):
        self.queue.put(topic_and_message)

    def flush(self):
        self.flush_complete_event.clear()
        self.queue.put(self.flush_marker)
        self.flush_complete_event.wait()

    def close(self):
        self.queue.put(self.stop_marker)
        self.queue.close()
        self.async_process.join()
        self.queue = None
        self.async_process = None

    def _consume_async_queue(self, queue, flush_complete_event):
        item = queue.get()
        while item != self.stop_marker:
            if item == self.flush_marker:
                super(AsyncProducer, self).flush()
                flush_complete_event.set()
            else:
                topic_and_message = item
                super(AsyncProducer, self).publish(topic_and_message)
            item = queue.get()
        queue.close()
        super(AsyncProducer, self).close()


# prepapre needs to be in the module top level so it can be serialized for
# multiprocessing
def _prepare(envelope_and_message):
    return create_message(
        envelope_and_message.envelope.pack(envelope_and_message.message)
    )


class _KafkaProducer(object):
    message_limit = 5000
    time_limit = 0.1

    @cached_property
    def envelope(self):
        return Envelope()

    def __init__(self, use_work_pool=True):
        self.kafka_client = get_kafka_client()

        self.use_work_pool = use_work_pool
        if use_work_pool:
            self.pool = Pool()
        self._reset_message_buffer()

    def publish(self, topic_and_message):
        self._add_message_to_buffer(topic_and_message)

        if self._is_ready_to_flush():
            self.flush_buffered_messages()

    def flush_buffered_messages(self):
        responses = self.kafka_client.send_produce_request(
            payloads=self._generate_produce_requests(),
            acks=1  # Written to disk on master
        )
        self._reset_message_buffer()

    def close(self):
        self.flush_buffered_messages()
        self.kafka_client.close()

        if self.use_work_pool:
            self.pool.close()
            self.pool.join()

    def _is_ready_to_flush(self):
        return (
            (time.time() - self.start_time) >= self.time_limit or
            self.message_buffer_size >= self.message_limit
        )

    def _add_message_to_buffer(self, topic_and_message):
        if self.use_work_pool:
            message = topic_and_message.message
        else:
            message = self._prepare_message(topic_and_message.message)

        self.message_buffer[topic_and_message.topic].append(message)
        self.message_buffer_size += 1

    def _generate_produce_requests(self):
        return [
            ProduceRequest(topic=topic, partition=0, messages=messages)
            for topic, messages in self._yield_prepared_topic_and_messages()
        ]

    def _yield_prepared_topic_and_messages(self):
        if self.use_work_pool:
            topics_and_messages_result = [
                (topic, self.pool.map_async(
                    _prepare,
                    [
                        EnvelopeAndMessage(envelope=self.envelope, message=message)
                        for message in messages
                    ]
                )) for topic, messages in self.message_buffer.iteritems()
            ]

            return ((topic, messages_result.get()) for topic, messages_result in topics_and_messages_result)
        else:
            return self.message_buffer.iteritems()

    def _prepare_message(self, message):
        return _prepare(EnvelopeAndMessage(envelope=self.envelope, message=message))

    def _reset_message_buffer(self):
        self.start_time = time.time()
        self.message_buffer = defaultdict(list)
        self.message_buffer_size = 0
