# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import socket

from cached_property import cached_property

from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline.lazy_message import LazyMessage
from data_pipeline.message_type import _ProtectedMessageType


class Client(object):
    """The client superclasses both the Producer and Consumer, and is largely
    responsible for shared producer/consumer registration responsibilities.

    DATAPIPE-157
    """

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, client_id):
        self._id = client_id

    def __init__(self, client_id='007'):
        self.monitoring_message = _MonitoringMessage(client_id)
        self.id = client_id


class _MonitoringMessage(object):
    """The class that implements functionality of monitoring the messages published/
    consumed by clients using kafka.
    """

    @property
    def window_width(self):
        """The duration over which monitoring_message will monitor (i.e count
        the number of messages) published to kafka

        TODO: This is set to a random value. Have to discuss this with team
        and finalize a value
        """
        return 1000

    @property
    def global_start_time(self):
        """Start time when monitoring_message for every client will start
        monitoring the number of messages produced/consumed

        TODO: Have to discuss with the team to finalize a value. One option
        could be to set it to the UNIX time when the project is deployed
        """
        return 0

    @cached_property
    def message(self):
        """Message containing monitoring information about the number
        of messages produced/consumed by the client in the given time frame
        """
        return LazyMessage(
            str('monitor-log'),
            0,
            self.record,
            _ProtectedMessageType.monitor
        )

    @property
    def publisher(self):
        """Publisher responsible for publishing monitoring information into
        Kafka
        """
        return self._publisher

    @publisher.setter
    def publisher(self, publisher):
        self._publisher = publisher

    @property
    def record(self):
        return self._record

    @record.setter
    def record(self, record):
        self._record = record

    def publish(self):
        """Publishing the results stored in the monitoring_message using
        the monitoring_publisher
        """
        self.publisher.publish(self.message)

    def reset_record(self, updated_start_timestamp):
        self.record['message_count'] = 1
        self.record['start_timestamp'] = updated_start_timestamp

    @property
    def message_count(self):
        """Number of messages produced/consumed by the client
        """
        return self.record['message_count']

    @message_count.setter
    def message_count(self, message_count):
        self.record['message_count'] = message_count

    def _notify_messages_published(self, position_data):
        """Called to notify the client of successfully published messages.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        # logger.debug("Client notified of new messages")
        self.position_data = position_data

    def __init__(self, client_id):
        self.publisher = LoggingKafkaProducer(self._notify_messages_published)
        self.record = dict(
            topic=str("my-topic"),  # TODO: needs to come from messages produced
            client_type='Producer',
            message_count=0,
            start_timestamp=self.global_start_time,
            host_info=socket.gethostname(),
            client_id=client_id
        )
