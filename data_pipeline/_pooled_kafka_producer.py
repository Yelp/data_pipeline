# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from multiprocessing import Pool

from data_pipeline._kafka_producer import _EnvelopeAndMessage
from data_pipeline._kafka_producer import _prepare
from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline.config import get_config


logger = get_config().logger


class PooledKafkaProducer(LoggingKafkaProducer):
    """PooledKafkaProducer extends KafkaProducer to use a pool of subprocesses
    to schematize and pack envelopes, instead of performing those operations
    synchronously.  Parallelizing and backgrounding these expensive operations
    can result in a substantial performance improvement.

    See the Quick Start for more information about choosing an appropriate
    producer.

    TODO(DATAPIPE-171|justinc): Actually write a Quick Start
    """

    def __init__(self, *args, **kwargs):
        self.pool = Pool()
        super(PooledKafkaProducer, self).__init__(*args, **kwargs)

    def close(self):
        logger.debug("Starting to close pooled producer")
        super(PooledKafkaProducer, self).close()
        logger.debug("Closing the pool")
        assert self.message_buffer_size == 0
        self.pool.close()
        # Joining pools can be flakey in CPython 2.6, and the message buffer
        # size is zero here, so terminating the pool is safe and ensure that
        # join always works.
        self.pool.terminate()
        self.pool.join()

    def _prepare_message(self, message):
        """This happens in the pool, so this is a noop"""
        return message

    def _generate_prepared_topic_and_messages(self):
        # The setup here isn't great, it's probably worth switching this to
        # keep the buffer in an array, then map it here.  It'd also be worth
        # looking at pipelining this, so there would be a regular buffer, and a
        # buffer that was being prepared.
        #
        # That would look like:
        #
        # publish -> accumulate in buffer -> move to prep area -> async prepare
        #   -> publish to kafka
        topics_and_messages_result = [
            (topic, self.pool.map_async(
                _prepare,
                [
                    _EnvelopeAndMessage(envelope=self.envelope, message=message)
                    for message in messages
                ]
            )) for topic, messages in self.message_buffer.iteritems()
        ]

        return [(topic, messages_result.get()) for topic, messages_result in topics_and_messages_result]
