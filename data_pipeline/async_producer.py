# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Event
from multiprocessing import Process
from multiprocessing import Queue

from data_pipeline.producer import Producer


class AsyncProducer(Producer):
    """AsyncProducer wraps the Producer making many of the operations
    asynchronous.  The AsyncProducer uses a python multiprocessing queue
    to pass messages to a subprocess.  The subprocess is responsible for
    publishing messages to Kafka.

    See :class:`data_pipeline.producer.Producer` for more information.

    Args:
        use_work_pool (bool): If true, the background process will use a
            multiprocessing pool to serialize messages in preperation for
            transport.  Default is false.
    """
    _stop_marker = 0
    _flush_marker = 1
    _max_queue_size = 10000  # Double the number of messages to buffer

    def __init__(self, *args, **kwargs):
        super(AsyncProducer, self).__init__(*args, **kwargs)

        self.queue = Queue(self._max_queue_size)

        # This event is used to block the foreground process until the
        # background process is able to finish flushing, since flush should
        # be a syncronous operation
        self.flush_complete_event = Event()

        self.async_process = Process(target=self._consume_async_queue, args=(self.queue, self.flush_complete_event))
        self.async_process.start()

    def publish(self, message):
        """Asynchronously enqueues a message to be published in a background
        process.  Messages are published after a number of messages are
        accumulated or after a slight time delay, whichever passes first.
        Passing a message to publish does not guarantee that it will be
        successfully published into Kafka.

        **TODO**:

        * Point to information about the message accumulation and time
          delay config.
        * Include information about checking which messages have actually
          been published.

        Args:
            message (data_pipeline.message.Message): message to publish
        """
        self.queue.put(message)

    def ensure_messages_published(self, messages, topic_offsets):
        """See :meth:`data_pipeline.producer.Producer.ensure_messages_published`"""
        raise NotImplementedError

    def flush(self):
        """See :meth:`data_pipeline.producer.Producer.flush`"""
        self.flush_complete_event.clear()
        self.queue.put(self._flush_marker)
        self.flush_complete_event.wait()

    def close(self):
        """Closes the producer, flushing all buffered messages into Kafka.
        Calling this method directly is not recommended, instead, use the
        producer as a context manager::

            with AsyncProducer() as producer:
                producer.publish(message)
                ...
                producer.publish(message)

        This call will block until all messages are successfully flushed.
        """
        self.queue.put(self._stop_marker)
        self.queue.close()
        self.async_process.join()
        self.queue = None
        self.async_process = None

    def get_checkpoint_position_data(self):
        """See :meth:`data_pipeline.producer.Producer.get_checkpoint_position_data`"""
        raise NotImplementedError

    def _consume_async_queue(self, queue, flush_complete_event):
        # TODO: the gets should timeout and call wake periodically
        item = queue.get()
        while item != self._stop_marker:
            if item == self._flush_marker:
                super(AsyncProducer, self).flush()
                flush_complete_event.set()
            else:
                message = item
                super(AsyncProducer, self).publish(message)
            item = queue.get()
        queue.close()
        super(AsyncProducer, self).close()
