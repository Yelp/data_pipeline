# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Event
from multiprocessing import Manager
from multiprocessing import Process
from multiprocessing import Queue

from data_pipeline.config import logger
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
    # TODO(DATAPIPE-154|justinc) This should be configured with staticconf
    _max_queue_size = 10000  # Double the number of messages to buffer

    def __init__(self, *args, **kwargs):
        super(AsyncProducer, self).__init__(*args, **kwargs)

        self.manager = Manager()
        self.shared_async_data = self.manager.Namespace()
        self.queue = Queue(self._max_queue_size)

        # This event is used to block the foreground process until the
        # background process is able to finish flushing, since flush should
        # be a synchronous operation
        self.flush_complete_event = Event()

        self.async_process = Process(
            target=self._async_runner,
            args=(self.queue, self.flush_complete_event, self.shared_async_data)
        )
        self.async_process.start()

        # Flushing here is really just a convenient way to wait for the child
        # process to start responding.  This also ensures that
        # `get_checkpoint_position_data` will have position data to return
        # as soon as the producer is created.  A wake method could potentially
        # fit the bill for this, but waking shouldn't be synchronous, and it
        # would add additional unnecessary complexity.
        self.flush()

    def publish(self, message):
        """Asynchronously enqueues a message to be published in a background
        process.  Messages are published after a number of messages are
        accumulated or after a slight time delay, whichever passes first.
        Passing a message to publish does not guarantee that it will be
        successfully published into Kafka.

        **TODO(DATAPIPE-155|justinc)**:

        * Point to information about the message accumulation and time
          delay config.
        * Include information about checking which messages have actually
          been published.

        Args:
            message (data_pipeline.message.Message): message to publish
        """
        self.queue.put(message)

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
        return self.shared_async_data.position_data

    def _async_runner(self, queue, flush_complete_event, shared_async_data):
        # See "Explicitly pass resources to child processes" under
        # https://docs.python.org/2/library/multiprocessing.html#all-platforms
        # for an explanation of why data used in the child process is passed
        # in, instead of just accessing it through self
        try:
            self.background_shared_async_data = shared_async_data
            self._consume_async_queue(queue, flush_complete_event)
        except:
            # TODO(DATAPIPE-153|justinc): Logging is a start here, but anything
            # that will kill this process should cause some kind of exception
            # in the sync process
            logger.exception("Runner Subprocess Crashed")

    def _consume_async_queue(self, queue, flush_complete_event):
        # TODO(DATAPIPE-156|justinc): the gets should timeout and call wake
        # periodically
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

    def _notify_messages_published(self, position_data):
        """Called to notify the producer of successfully published messages.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        self.background_shared_async_data.position_data = position_data
