# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing
import os
import signal
from multiprocessing import Event
from multiprocessing import Manager
from multiprocessing import Process
from multiprocessing import Queue

from data_pipeline.config import get_config
from data_pipeline.producer import Producer


logger = get_config().logger


class UnrecoverableSubprocessError(Exception):
    """Error occurs when the AsyncProducer background process encounters a fault
    that it is unable to recover from.  This error is meant to be fatal.
    Processes that receive it should die, and follow their recovery procedure
    when they come back online.  These faults are serious, check the log files
    and see what caused the background process to die.

    Hint:

        grep -A 30 "AsyncProducer Subprocess Crashed"
    """
    pass


class AsyncProducer(Producer):
    """AsyncProducer wraps the Producer making many of the operations
    asynchronous.  The AsyncProducer uses a python multiprocessing queue
    to pass messages to a subprocess.  The subprocess is responsible for
    publishing messages to Kafka.

    See :class:`data_pipeline.producer.Producer` for more information.

    Args:
        use_work_pool (bool): If true, the background process will use a
            multiprocessing pool to serialize messages in preparation for
            transport.  Default is false.
    """
    _stop_marker = 0
    _flush_marker = 1
    # TODO(DATAPIPE-154|justinc) This should be configured with staticconf
    _max_queue_size = 10000  # Double the number of messages to buffer

    def __init__(self, *args, **kwargs):
        super(AsyncProducer, self).__init__(*args, **kwargs)

        logger.debug("Async producer initialized")

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

        # Register a signal handler for the child, this is used to signal that
        # the child has crashed.
        signal.signal(signal.SIGUSR1, self._child_handler)

        logger.debug("Starting async process")
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
        self.monitoring_message.monitor(message)

    def flush(self):
        """See :meth:`data_pipeline.producer.Producer.flush`"""
        self.flush_complete_event.clear()
        logger.debug("Pushing flush marker")
        self.queue.put(self._flush_marker)
        logger.debug("Waiting for flush complete event...")
        self.flush_complete_event.wait()
        logger.debug("Flush complete.")

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
        if not self.async_process.is_alive():
            # This shouldn't happen because of signaling, but if it does,
            # it is a bug that needs to be fixed.
            logger.error("Background process has already died.")

        # Clear the queue
        self.flush()

        # Send the stop marker and wait for the queue to die
        self.queue.put(self._stop_marker)
        self.queue.close()
        self.queue.join_thread()

        # Wait for the subprocess to exit
        self.async_process.join()

        # Cleanup background processes
        self.queue = None
        self.async_process = None
        self.manager.shutdown()
        self.manager = None

        # We should never be leaving processes behind
        assert len(multiprocessing.active_children()) == 0

    def get_checkpoint_position_data(self):
        """See :meth:`data_pipeline.producer.Producer.get_checkpoint_position_data`"""
        return self.shared_async_data.position_data

    def wake(self):
        """The AsyncProducer's wake function is a noop.  The background process
        is capable of periodically waking itself to publish messages, and does
        so automatically.  There is no need to call this function.
        """
        pass

    def _async_runner(self, queue, flush_complete_event, shared_async_data):
        # See "Explicitly pass resources to child processes" under
        # https://docs.python.org/2/library/multiprocessing.html#all-platforms
        # for an explanation of why data used in the child process is passed
        # in, instead of just accessing it through self
        logger.debug("Producer subprocess started")
        try:
            self.background_shared_async_data = shared_async_data
            self._consume_async_queue(queue, flush_complete_event)
        except:
            # TODO(DATAPIPE-153|justinc): Logging is a start here, but anything
            # that will kill this process should cause some kind of exception
            # in the sync process
            logger.exception("AsyncProducer Subprocess Crashed")
            # This doesn't actually kill the parent, it signals that the child
            # process has died.  Signalling here interrupts whatever else is
            # going on, in particular, if the parent process is sleeping waiting
            # for a flush to complete, this will wake it.  SIGCHLD can't be used
            # here because the multiprocessing.Manager uses it internally, so
            # adding a handler to that breaks the Manager.
            os.kill(os.getppid(), signal.SIGUSR1)

    def _consume_async_queue(self, queue, flush_complete_event):
        # TODO(DATAPIPE-156|justinc): the gets should timeout and call wake
        # periodically
        logger.debug("Waiting for jobs from queue")
        item = queue.get()
        while item != self._stop_marker:
            if item == self._flush_marker:
                logger.debug("Recieved flush marker")
                super(AsyncProducer, self).flush()
                flush_complete_event.set()
            else:
                message = item
                super(AsyncProducer, self).publish_message(message)
            item = queue.get()
        logger.debug("Shutting down the background producer")
        queue.close()
        super(AsyncProducer, self).close()

    def _set_kafka_producer_position(self, position_data):
        """Called periodically to update the producer with position data.  This
        is expected to be called at least once when the KafkaProducer is started,
        and whenever messages are successfully published.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        self.background_shared_async_data.position_data = position_data

    def _child_handler(self, signum, frame):
        """This method is activated by the USR1 signal when the child process
        exits uncleanly.  The intention is for it to stop the parent, letting
        the parent know that a fault happened in the subprocess.  This error is
        meant to be fatal, processes that receive it should die, then follow
        their recovery procedure.  justinc considered propogating the exception
        from the child to the parent process, but choose not to since it's
        likely to be error prone (e.g. what happens if we try to use a manager
        to do that, but the fault is that the manager crashed).
        """
        # TODO(DATAPIPE-166|justinc): Trigger an alert here
        raise UnrecoverableSubprocessError(
            "The subprocess crashed!  See the logs to figure out what happened."
        )
