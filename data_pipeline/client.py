# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import socket

from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline.config import get_config
from data_pipeline.lazy_message import LazyMessage
from data_pipeline.message_type import _ProtectedMessageType


logger = get_config().logger


class Client(object):
    """The client superclasses both the Producer and Consumer, and is largely
    responsible for shared producer/consumer registration responsibilities.

    Args:
        client_type (str): type of the client. Can be producer or consumer
        client_name (str): Name associated with the client
    DATAPIPE-157
    """

    def __init__(self, client_type, client_name):
        self.monitoring_message = _Monitor(client_type, client_name)
        self.client_name = client_name


class _Monitor(object):
    """The class that implements functionality of monitoring the messages published/
    consumed by clients using kafka.

    In current implementation, monitoring_window_in_sec is stored in the staticconf
    and is the same for every client. As one of the future features, one could add
    the option to have a custom monitoring_window_in_sec for every client and choose
    the appropriate duration based on the number of messages published by the client.

    Args:
        client_type (str): type of the client the _Monitor object is associated to.
            could be either producer or consumer
        client_name (str): name of the associated client
        start_time (int): start_time when _Monitor object will start counting the
            number of messages produced/consumed by the associated client

    """

    def __init__(self, client_type, client_name, start_time=0):
        self.topic_to_tracking_info_map = {}
        self.client_type = client_type
        self.client_id = self._lookup_client_id(client_name)
        self._monitoring_window_in_sec = get_config().monitoring_window_in_sec
        self.start_time = start_time
        self.producer = LoggingKafkaProducer(self._notify_messages_published)

    def _lookup_client_id(self, client_name):
        """Returns the client_id associated with the client_name.

        TODO(DATAPIPE-273|pujun): Need to implement the functionality
        to find the client_id of a client from its client_name.
        """
        return 7

    def _get_message(self, topic):
        """Message containing monitoring information about the number
        of messages produced/consumed by the client in the given time frame
        """
        return LazyMessage(
            str(topic + '-monitor-log'),
            0,
            self._get_record(topic),
            _ProtectedMessageType.monitor
        )

    def _get_default_record(self, topic):
        """Creates and returns the default version of the topic_to_tracking_info_map
        """
        self.topic_to_tracking_info_map[topic] = {
            'topic': topic,
            'client_type': self.client_type,
            'message_count': 0,
            'start_timestamp': self.start_time,
            'host_info': socket.gethostname(),
            'client_id': self.client_id
        }
        return self.topic_to_tracking_info_map[topic]

    def _get_record(self, topic):
        """returns the record associated with the topic for any client
        If the topic has no record, a new record is created and returned
        """
        if topic in self.topic_to_tracking_info_map:
            return self.topic_to_tracking_info_map[topic]
        else:
            return self._get_default_record(topic)

    def _publish(self, topic):
        """Publishing the results stored in the monitoring_message using
        the monitoring_publisher
        """
        self.producer.publish(self._get_message(topic))

    def _reset_record(self, topic, timestamp):
        """resets the record for a particular topic by resetting
        message_count to 1 (since the program has already published one
        message) and updating the start_timestamp to the start_timestamp
        of the newly published message
        """
        self._get_default_record(topic)
        self._get_record(topic)['start_timestamp'] = self._get_updated_start_timestamp(timestamp)

    def _get_updated_start_timestamp(self, timestamp):
        """
        returns the new start_timestamp based on the value of the existing timestamp of the
        encountered message.

        For instance, if message_timestamp is 2013 and the monitoring_window_in_sec is 1000,
        2000 will be returned
        """
        return (timestamp / self._monitoring_window_in_sec) * self._monitoring_window_in_sec

    def get_message_count(self, topic):
        """Number of messages produced/consumed by the client
        """
        return self._get_record(topic)['message_count']

    def _increment_message_count(self, topic):
        """Increments the message_count in the record for the
        topic.
        """
        self._get_record(topic)['message_count'] += 1

    def _notify_messages_published(self, position_data):
        """Called to notify the client of successfully published messages.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        logger.debug("Client " + str(self.client_id) + " published its monitoring message")

    def record_message(self, message):
        """Used to handle the logic of recording monitoring_message in kafka and resetting
        it if necessary
        """
        if self._get_record(message.topic)['start_timestamp'] + self._monitoring_window_in_sec < message.timestamp:
            self._publish(message.topic)
            self._reset_record(message.topic, message.timestamp)
        self._increment_message_count(message.topic)
