# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from Queue import Full
from time import sleep
from traceback import format_exc

from yelp_kafka.consumer import KafkaConsumerBase

from data_pipeline.config import get_config
from data_pipeline.message import create_from_kafka_message

logger = get_config().logger


class KafkaConsumerWorker(KafkaConsumerBase):

    DEFAULT_SLEEP_TIME = 0.1

    def __init__(
            self,
            topic,
            config,
            partitions,
            message_buffer,
            decode_payload
    ):
        """ KafkaConsumerWorker is the workhorse behind the
        MultiprocessingConsumerGroup that powers the
        ``data_pipeline.consumer.Consumer`` class. It handles receiving a
        message from Kafka, decoding the envelope, constructing a
        ``data_pipeline.message.Message``, and (optionally) decoding the payload
        data all asynchronously before adding it to a shared Queue which the
        user of  ``data_pipeline.consumer.Consumer`` pulls messages from.

        :param str topic: Kafka topic name.
        :param yelp_kafka.config.KafkaConsumerConfig config:
        :param [] partitions: topic partitions to consumer from.
        :param multiprocessing.Queue message_buffer: Queue to put messages into
        :param boolean decode_payload: Determine if the worker is responsible
        for decoding the payload data
        :return:
        """
        super(KafkaConsumerWorker, self).__init__(
            topic,
            config,
            partitions
        )
        self.message_buffer = message_buffer
        self.decode_payload = decode_payload

    def initialize(self):
        pass

    def dispose(self):
        self.message_buffer.close()

    def process(self, kafka_message):
        try:
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
                sleep(self.DEFAULT_SLEEP_TIME)

    @staticmethod
    def create_factory(
            message_buffer,
            decode_payload
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
                decode_payload=decode_payload
            )
        return create_worker
