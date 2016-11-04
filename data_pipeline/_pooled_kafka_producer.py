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
        try:
            logger.debug("Starting to close pooled producer")
            super(PooledKafkaProducer, self).close()
            assert self.message_buffer_size == 0
            logger.debug("Closing the pool")
            self.pool.close()
            logger.debug("Pool is closed.")
        except:
            logger.error("Exception occurred when closing pooled producer.")
            raise
        finally:
            # The processes in the pool should be cleaned up in all cases. The
            # exception will be re-thrown if there is one.
            #
            # Joining pools can be flaky in CPython 2.6, and the message buffer
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
        #
        # clin proposed an alternative approach:
        # I personally would prefer to pipeline this, and spawn certain number
        # of workers doing schematizing and packing messages (listen to regular
        # queue and move items from it to schematized queue), and another
        # certain number of workers sending messages to kafka (listen to
        # schematized queue and move items from it to kafka topic). The number
        # of workers can be configurable so we can treak performance if needed.
        # One benefit is we can build some metric from it and see the throughput
        # or identify where the bottleneck is if there is one.
        #
        # Also, in this way, it seems the schematizing workers don't have to
        # work in bulk and can continuously schematize and pack the incoming raw
        # messages, and move them into schemaized/packed queue (if there are
        # free workers). The send-requests workers can then send the messages
        # in bulk or every certain amount of time. The down side is this is a
        # more complicated approach.
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
