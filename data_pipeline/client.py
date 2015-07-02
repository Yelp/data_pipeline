# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cached_property import cached_property

from data_pipeline.lazy_message import LazyMessage
from data_pipeline.message_type import MessageType
from data_pipeline.monitoring_publisher import MonitoringPublisher


class Client(object):
    """The client superclasses both the Producer and Consumer, and is largely
    responsible for shared producer/consumer registration responsibilities.

    DATAPIPE-157
    """

    @property
    def MONITORING_WINDOW_WIDTH(self):
        """The duration over which monitoring_message will monitor (i.e count
        the number of messages) published to kafka

        TODO: This is set to a random value. Have to discuss this with team
        and finalize a value
        """
        return 1000

    @property
    def GLOBAL_MONITORING_START_TIME(self):
        """Start time when monitoring_message for every client will start
        monitoring the number of messages produced/consumed

        TODO: Have to discuss with the team to finalize a value. One option
        could be to set it to the UNIX time when the project is deployed
        """
        return 0

    @cached_property
    def monitoring_message(self):
        """Message containing monitoring information about the number
        of messages produced/consumed by the client in the given time frame
        """
        return LazyMessage(str('monitor-log'), 0, self.monitoring_dict, MessageType.monitor)

    @property
    def monitoring_publisher(self):
        """Publisher responsible for publishing monitoring information into
        Kafka
        """
        return self._monitoring_publisher

    @monitoring_publisher.setter
    def monitoring_publisher(self, monitoring_publisher):
        self._monitoring_publisher = monitoring_publisher

    @property
    def monitoring_dict(self):
        return self._monitoring_dict

    @monitoring_dict.setter
    def monitoring_dict(self, monitoring_dict):
        self._monitoring_dict = monitoring_dict

    def publish_monitoring_results(self):
        """Publishing the results stored in the monitoring_message using
        the monitoring_publisher
        """
        self.monitoring_publisher.publish(self.monitoring_message)

    def reset_monitoring_dict(self):
        self.monitoring_dict['message_count'] = 1
        self.monitoring_dict['start_timestamp'] += 1

    @property
    def message_count(self):
        """Number of messages produced/consumed by the client
        """
        return self.monitoring_dict['message_count']

    @message_count.setter
    def message_count(self, message_count):
        self.monitoring_dict['message_count'] = message_count

    def __init__(self, monitoring_message=None):
        self.monitoring_message = monitoring_message
        self.monitoring_publisher = MonitoringPublisher()
