# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import socket

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
        """id associated with the client."""
        return self._id

    @id.setter
    def id(self, client_id):
        self._id = client_id

    def __init__(self, client_type, client_id='007'):
        self.monitoring_message = _Monitor(client_type, client_id)
        self.id = client_id


class _Monitor(object):
    """The class that implements functionality of monitoring the messages published/
    consumed by clients using kafka.

    Args:
        client_type: type of the client the monitoring_message is associated to.
            could be either producer or consumer
        client_id: id of the associated client
        window_width: the duration (in second) for which monitoring_message
             will monitor
        start_time: start_time when monitoring_message for client will start
            counting the number of messages produced/consumed
    """

    def message(self, topic):
        """Message containing monitoring information about the number
        of messages produced/consumed by the client in the given time frame
        """
        return LazyMessage(
            str(topic + '-monitor-log'),
            0,
            self.record[topic],
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

    def get_default_record(self, topic):
        self.record[topic] = {
            'topic': topic,
            'client_type': self.client_type,
            'message_count': 0,
            'start_timestamp': self.start_time,
            'host_info': socket.gethostname(),
            'client_id': self.client_id
        }
        return self.record[topic]

    def get_record(self, topic):
        """returns the record associated with the topic for any client.
        If the topic has no record, a new record is created and returned.
        """
        if topic in self.record:
            return self.record[topic]
        else:
            return self.get_default_record(topic)

    def publish(self, topic):
        """Publishing the results stored in the monitoring_message using
        the monitoring_publisher
        """
        self.producer.publish(self.message(topic))

    def reset_record(self, topic, timestamp):
        """resets the record for a particular topic by resetting
        message_count to 1 (since the program has already published one
        message) and updating the start_timestamp to the start_timestamp
        of the newly published message
        """
        self.get_default_record(topic)
        self.get_record(topic)['start_timestamp'] = self.get_updated_start_timestamp(timestamp)

    def get_updated_start_timestamp(self, timestamp):
        return (timestamp / self.monitoring_width_in_sec) * self.monitoring_width_in_sec

    def get_message_count(self, topic):
        """Number of messages produced/consumed by the client
        """
        return self.get_record(topic)['message_count']

    def increment_message_count(self, topic):
        """Increments the message_count in the record for the
        topic.
        """
        self.get_record(topic)['message_count'] += 1

    def _notify_messages_published(self, position_data):
        """Called to notify the client of successfully published messages.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        # logger.debug("Client notified of new messages")
        self.position_data = position_data

    def monitor(self, message):
        if self.get_record(message.topic)['start_timestamp'] + self.monitoring_width_in_sec < message.timestamp:
            self.publish(message.topic)
            self.reset_record(message.topic, message.timestamp)
        self.increment_message_count(message.topic)

    def __init__(self, client_type, client_id, monitoring_width_in_sec=1000, start_time=0):
        self.producer = LoggingKafkaProducer(self._notify_messages_published)
        self.record = {}
        self.client_type = client_type
        self.client_id = client_id
        self.monitoring_width_in_sec = monitoring_width_in_sec
        self.start_time = start_time
