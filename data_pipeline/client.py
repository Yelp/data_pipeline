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
        self.monitoring_message = _MonitoringMessage(client_type, client_id)
        self.id = client_id


class _MonitoringMessage(object):
    """The class that implements functionality of monitoring the messages published/
    consumed by clients using kafka.

    Args:
        client_type: type of the client the monitoring_message is associated to.
            could be either producer or consumer
        client_id: id of the associated client
        window_width: the duration for which monitoring_message will monitor
        start_time: start_time when monitoring_message for client will start
            counting the number of messages produced/consumed
    """

    @property
    def window_width(self):
        """The duration for which monitoring_message will monitor (i.e count
        the number of messages) published to kafka

        TODO: This is set to a random value. Have to discuss this with team
        and finalize a value
        """
        return self._window_width

    @window_width.setter
    def window_width(self, window_width):
        self._window_width = window_width

    @property
    def start_time(self):
        """Start time when monitoring_message for client will start
        monitoring the number of messages produced/consumed

        TODO: Have to discuss with the team to finalize a value. One option
        could be to set it to the UNIX time when the project is deployed
        """
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

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

    def get_record(self, topic):
        """returns the record associated with the topic for any client.
        If the topic has no record, a new record is created and returned.
        """
        if topic in self.record:
            return self.record[topic]
        else:
            print "record for topic ", topic, " being created"
            self.record[topic] = dict(
                topic=topic,
                client_type=self.client_type,
                message_count=0,
                start_timestamp=self.start_time,
                host_info=socket.gethostname(),
                client_id=self.client_id
            )
            return self.record[topic]

    def publish(self, topic):
        """Publishing the results stored in the monitoring_message using
        the monitoring_publisher
        """
        self.publisher.publish(self.message(topic))

    def reset_record(self, topic, updated_start_timestamp):
        """resets the record for a particular topic by resetting
        message_count to 1 (since the program has already published one
        message) and updating the start_timestamp to the start_timestamp
        of the newly published message
        """
        self.record[topic]['message_count'] = 1
        self.record[topic]['start_timestamp'] = updated_start_timestamp

    def get_message_count(self, topic):
        """Number of messages produced/consumed by the client
        """
        return self.record[topic]['message_count']

    def increment_message_count(self, topic):
        """Increments the message_count in the record for the
        topic.
        """
        self.record[topic]['message_count'] += 1

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
        if self.get_record(message.topic)['start_timestamp'] + self.window_width < message.timestamp:
            self.publish(message.topic)
            self.reset_record(message.topic, message.timestamp)
        else:
            self.increment_message_count(message.topic)

    def __init__(self, client_type, client_id, window_width=1000, start_time=0):
        self.publisher = LoggingKafkaProducer(self._notify_messages_published)
        self.record = {}
        self.client_type = client_type
        self.client_id = client_id
        self.window_width = window_width
        self.start_time = start_time
