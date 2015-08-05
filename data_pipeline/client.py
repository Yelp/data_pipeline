# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import socket

from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline.config import get_config
from data_pipeline.message import MonitorMessage


logger = get_config().logger


class Client(object):
    """The client superclasses both the Producer and Consumer, and is largely
    responsible for shared producer/consumer registration responsibilities and
    monitoring.

    Note:

        Client will be responsible for producer/consumer registration,
        which will be implemented in DATAPIPE-157.

    Args:
        client_name (str): Name associated with the client - this name will
            be used as the client's identifier for producer/consumer
            registration.  This identifier should be treated as a constant
            once defined, and should be unique per use case and application.

            For example, if a team has a service with two consumers, each
            filling a different role, and a consumer in yelp main, all three
            should have different client names so their activity can be
            differentiated.  Multiple instances of a producer or consumer in the
            same application should share a client name.  If an application is
            both consuming and producing for the same purpose (i.e. transforming
            and republishing a topic), the consumer and producer should share a
            client name, otherwise the client name should be unique across
            logical applications.

            There is no need to use different client names in different
            environments, but we do suggest namespacing.  For example,
            `services.yelp-main.datawarehouse.rich-transformers` or
            `services.user_tracking.review_tracker`.
        team (str): Team name, as defined in `sensu_handlers::teams` (see
            y/sensu-teams).  notification_email must be defined for a team to be
            registered as a producer or consumer.  This information will be used
            to communicate about data changes impacting the client.

            `sensu_handlers::teams` is the canonical data source for team
            notifications, and its usage was recommended by ops.  It was also
            adopted for usage in ec2 instance tagging in y/cep427.
        expected_frequency (int): How frequently, in seconds, that the client
            expects to run to produce or consume messages.  Any positive
            integer value can be used, but some common constants have been
            defined in :class:`data_pipeline.expected_frequency.ExpectedFrequency`.

            See :class:`data_pipeline.expected_frequency.ExpectedFrequency` for
            additional detail.

    Raises:
        InvalidTeamError: If the team specified is either not defined or does
            not have a notification_email registered.  Ops deputies can modify
            `sensu_handlers::teams` in puppet to add a team.
    """

    def __init__(self, client_name, team, expected_frequency):
        self.monitoring_message = _Monitor(client_name, type(self).__name__.lower())
        self.client_name = client_name


class _Monitor(object):
    """The class implements functionality of monitoring messages which are
    published/consumed by clients using kafka.

    In current implementation, monitoring_window_in_sec is stored in the staticconf
    and is the same for every client. As one of the future features, one could add
    the option to have a custom monitoring_window_in_sec for every client and choose
    the appropriate duration based on the average number of messages processed by
    the client.

    Args:
        client_name (str): name of the associated client
        client_type (str): type of the client the _Monitor object is associated to.
        Could be either producer or consumer
        start_time (int): start_time when _Monitor object will start counting the
            number of messages produced/consumed by the associated client
    """

    def __init__(self, client_name, client_type, start_time=0):
        self.topic_to_tracking_info_map = {}
        self.client_name = client_name
        self.client_type = client_type
        self._monitoring_window_in_sec = get_config().monitoring_window_in_sec
        self.start_time = start_time
        self.producer = LoggingKafkaProducer(self._notify_messages_published)
        self.monitoring_schema_id = self._get_monitoring_schema_id()

    def _get_default_record(self, topic):
        """Returns the default version of the topic_to_tracking_info_map entry
        """
        return {
            'topic': topic,
            'client_type': self.client_type,
            'message_count': 0,
            'start_timestamp': self.start_time,
            'host_info': socket.gethostname(),
            'client_name': self.client_name
        }

    def _get_record(self, topic):
        """returns the record associated with the topic for any client
        If the topic has no record, a new record is created and returned
        """
        tracking_info = self.topic_to_tracking_info_map.get(topic)
        if tracking_info is None:
            tracking_info = self._get_default_record(topic)
            self.topic_to_tracking_info_map[topic] = tracking_info
        return tracking_info

    def _publish(self, tracking_info):
        """publish monitoring results, stored in the monitoring_message, using
        the producer
        """
        self.producer.publish(
            MonitorMessage(
                topic=str('message-monitoring-log'),
                schema_id=self.monitoring_schema_id,
                payload_data=tracking_info,
            )
        )

    def _get_monitoring_schema_id(self):
        """Returns the schema used to encode the payload.
        TODO(DATAPIPE-274|pujun): return the schema_id associated with
        the monitoring_message avro schema
        """
        return 0

    def _reset_monitoring_record(self, tracking_info):
        """resets the record for a particular topic by resetting
        message_count to 0 and updating the start_timestamp to the
        starting point of the next monitoring_window
        """
        tracking_info['message_count'] = 0
        tracking_info['start_timestamp'] += self._monitoring_window_in_sec

    def _notify_messages_published(self, position_data):
        """Called to notify the client of successfully published messages.

        Args:
            position_data (:class:PositionData): PositionData structure
                containing details about the last messages published to Kafka,
                including Kafka offsets and upstream position information.
        """
        logger.info("Client: " + self.client_name + " published monitoring message")

    def record_message(self, message):
        """Used to handle the logic of recording monitoring_message in kafka and resetting
        it if necessary
        """
        tracking_info = self._get_record(message.topic)
        while tracking_info['start_timestamp'] + self._monitoring_window_in_sec < message.timestamp:
            self._publish(tracking_info)
            self._reset_monitoring_record(tracking_info)
        tracking_info['message_count'] += 1

    def flush_buffered_info(self):
        """Publishes the buffered information, stored in topic_to_tracking_info_map,
        to kafka and resets topic_to_tracking_info_map to an empty dictionary
        """
        for remaining_monitoring_topic, tracking_info in self.topic_to_tracking_info_map.items():
            self._publish(tracking_info)
        self.producer.flush_buffered_messages()
        self.topic_to_tracking_info_map = {}

    def close(self):
        """Called when the associated client is exiting/closing. Calls flush_buffered_info
        and also closes monitoring.producer.
        """
        self.flush_buffered_info()
        self.producer.close()
