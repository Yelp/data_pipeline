# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random
from Queue import Full
from time import sleep
from traceback import format_exc

from yelp_kafka.consumer import KafkaConsumerBase

from data_pipeline.config import get_config
from data_pipeline.message import create_from_kafka_message

logger = get_config().logger


class KafkaConsumerWorker(KafkaConsumerBase):

    def __init__(
            self,
            topic,
            config,
            partitions,
            message_buffer,
            decode_payload,
            min_sleep_time,
            max_sleep_time
    ):
        """ KafkaConsumerWorker is the workhorse behind the
        MultiprocessingConsumerGroup that powers the
        ``data_pipeline.consumer.Consumer`` class. It handles receiving a
        message from Kafka, decoding the envelope, constructing a
        ``data_pipeline.message.Message``, and (optionally) decoding the payload
        data all asynchronously before adding it to a shared Queue which the
        user of  ``data_pipeline.consumer.Consumer`` pulls messages from.

        Args:
            topic (str): Kafka topic name.
            config (yelp_kafka.config.KafkaConsumerConfig): Configuration for the
                kafka consumer.
            partitions ([int]): Topic partitions to consume from.
            message_buffer (multiprocessing.Queue): Queue to put messages into
            decode_payload (boolean): Determine if the worker is responsible
                for decoding the payload data
            min_sleep_time (float): Minimum time (in seconds) to sleep while
                waiting to add messages to the shared ``message_buffer``
            max_sleep_time (float): Maximum time (in seconds) to sleep while
                waiting to add messages to the shared ``message_buffer``
        """
        super(KafkaConsumerWorker, self).__init__(
            topic,
            config,
            partitions
        )
        if min_sleep_time > max_sleep_time or min_sleep_time < 0.0:
            raise ValueError(
                "The min_sleep_time ({0}) must not be greater than the "
                "max_sleep_time ({1}) and both must be positive.".format(
                    min_sleep_time,
                    max_sleep_time
                )
            )
        self.message_buffer = message_buffer
        self.decode_payload = decode_payload
        self.min_sleep_time_sec = min_sleep_time
        self.max_sleep_time_sec = max_sleep_time

    def initialize(self):
        pass

    def dispose(self):
        self.message_buffer.close()

    def process(self, kafka_message):
        try:
            # TODO(DATAPIPE-240|joshszep): Add support for specifying a reader schema_id
            message = create_from_kafka_message(
                topic=self.topic,
                kafka_message=kafka_message,
                force_payload_decoding=self.decode_payload
            )
        except Exception as exc:
            logger.error(format_exc(exc))
            raise
        while True:
            try:
                self.message_buffer.put_nowait(message)
                break
            except Full:
                sleep(random.uniform(
                    self.min_sleep_time_sec,
                    self.max_sleep_time_sec
                ))

    @staticmethod
    def create_factory(
            message_buffer,
            decode_payload,
            min_sleep_time,
            max_sleep_time
    ):
        """ Helper method to allow KafkaConsumerWorker be created
        by the MultiprocessingConsumerGroup, which requires providing a function
        that matches the KafkaConsumerBase.__init__ signature, to accept
        additional initialization arguments by way of a closure
        """
        def create_worker(topic, config, partitions):
            return KafkaConsumerWorker(
                topic=topic,
                config=config,
                partitions=partitions,
                message_buffer=message_buffer,
                decode_payload=decode_payload,
                min_sleep_time=min_sleep_time,
                max_sleep_time=max_sleep_time
            )
        return create_worker
