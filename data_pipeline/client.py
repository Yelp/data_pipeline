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

import math
import os
import socket
import time

import simplejson
from cached_property import cached_property

from data_pipeline._kafka_producer import LoggingKafkaProducer
from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import MonitorMessage
from data_pipeline.registrar import Registrar
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.team import Team


logger = get_config().logger


class Client(object):
    """The client superclasses both the Producer and Consumer, and is largely
    responsible for shared producer/consumer registration responsibilities and
    monitoring.

    Note:

        Client will be responsible for producer/consumer registration,
        which will be implemented in DATAPIPE-1154.

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
        team_name (str): Team name, as defined in `sensu_handlers::teams` (see
            y/sensu-teams).  notification_email must be defined for a team to be
            registered as a producer or consumer.  This information will be used
            to communicate about data changes impacting the client.

            `sensu_handlers::teams` is the canonical data source for team
            notifications, and its usage was recommended by ops.  It was also
            adopted for usage in ec2 instance tagging in y/cep427.
        expected_frequency_seconds (int, ExpectedFrequency): How frequently, in seconds,
            that the client expects to run to produce or consume messages.
            Any positive integer value can be used, but some common constants
            have been defined in
            :class:`data_pipeline.expected_frequency.ExpectedFrequency`.

            See :class:`data_pipeline.expected_frequency.ExpectedFrequency` for
            additional detail.
        monitoring_enabled (Optional[bool]): If true, monitoring will be enabled
            to record client's activities. Default is true.
        dry_run (Optional[bool]): If true, client will skip publishing message to
            kafka. Default is false.

    Raises:
        InvalidTeamError: If the team specified is either not defined or does
            not have a notification_email registered.  Ops deputies can modify
            `sensu_handlers::teams` in puppet to add a team.
    """

    def __init__(
        self,
        client_name,
        team_name,
        expected_frequency_seconds,
        monitoring_enabled=True,
        dry_run=False
    ):
        self.monitor = _Monitor(
            client_name,
            self.client_type,
            start_time=_Monitor.get_monitor_window_start_timestamp(
                time.time()
            ),
            monitoring_enabled=monitoring_enabled,
            dry_run=dry_run
        )
        self.client_name = client_name
        self.team_name = team_name
        self.expected_frequency_seconds = expected_frequency_seconds
        self.registrar = Registrar(
            team_name,
            client_name,
            self.client_type,
            self.expected_frequency_seconds
        )

    @property
    def client_name(self):
        """Name associated with the client"""
        return self._client_name

    @client_name.setter
    def client_name(self, client_name):
        if not client_name or not isinstance(client_name, (str, unicode)):
            raise ValueError("Client name must be non-empty text")
        self._client_name = client_name

    @property
    def team_name(self):
        """Team associated with the client"""
        return self._team_name

    @team_name.setter
    def team_name(self, team_name):
        if not Team.exists(team_name):
            raise ValueError(
                "Team name must exist: see the team_name argument at "
                "y/dp_client_docs for detailed information about adding a team."
            )
        self._team_name = team_name

    @property
    def expected_frequency_seconds(self):
        """How frequently, in seconds, that the client expects to run to produce
        or consume messages.
        """
        return self._expected_frequency_seconds

    @expected_frequency_seconds.setter
    def expected_frequency_seconds(self, expected_frequency_seconds):
        if isinstance(expected_frequency_seconds, ExpectedFrequency):
            expected_frequency_seconds = expected_frequency_seconds.value

        if not (isinstance(expected_frequency_seconds, int) and expected_frequency_seconds >= 0):
            raise ValueError("Client name must be non-empty text")
        self._expected_frequency_seconds = expected_frequency_seconds

    @property
    def client_type(self):
        """String identifying the client type."""
        raise NotImplementedError


class _Monitor(object):
    """The class implements functionality of monitoring messages which are
    published/consumed by clients using kafka.

    In current implementation, monitoring_window_in_sec is stored in the staticconf
    and is the same for every client. As one of the future features, one could add
    the option to have a custom monitoring_window_in_sec for every client and choose
    the appropriate duration based on the average number of messages processed by
    the client.

    Args:
        client_name (str): name of the associated client.
        client_type (str): type of the client the _Monitor is associated to.
            Could be either producer or consumer.
        start_time (int): unix start_time when _Monitor will start tracking
            the number of messages produced/consumed by the associated client.
        monitoring_enabled (Optional[bool]): If true, monitoring will be enabled
            to record client's activities. Default is true.
        dry_run (Optional[bool]): If true, client will skip publishing message to
            kafka. Default is false.
    """

    def __init__(
        self,
        client_name,
        client_type,
        start_time=0,
        monitoring_enabled=True,
        dry_run=False
    ):
        self.client_name = client_name
        self.client_type = client_type

        self.monitoring_enabled = monitoring_enabled
        if not self.monitoring_enabled:
            return

        self.topic_to_tracking_info_map = {}
        self._monitoring_window_in_sec = get_config().monitoring_window_in_sec
        self.start_time = start_time
        self.producer = LoggingKafkaProducer(
            self._notify_messages_published,
            dry_run=dry_run
        )
        self.dry_run = dry_run
        self._last_msg_timestamp = None

    @classmethod
    def get_monitor_window_start_timestamp(cls, timestamp):
        _monitor_window_in_sec = get_config().monitoring_window_in_sec
        return (int(math.floor(int(timestamp) / _monitor_window_in_sec)) *
                _monitor_window_in_sec)

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
                schema_id=self.monitor_schema_id,
                payload_data=tracking_info,
                dry_run=self.dry_run
            )
        )

    @cached_property
    def monitor_schema(self):
        return get_schematizer().register_schema(
            namespace=self._monitor_schema['namespace'],
            source=self._monitor_schema['name'],
            schema_str=simplejson.dumps(self._monitor_schema),
            source_owner_email='bam+data_pipeline+monitor@yelp.com',
            contains_pii=False
        )

    @cached_property
    def _monitor_schema(self):
        schema_file = os.path.join(
            os.path.dirname(__file__),
            'schemas/monitoring_message_v1.avsc'
        )
        with open(schema_file, 'r') as f:
            schema_string = f.read()
        return simplejson.loads(schema_string)

    @cached_property
    def monitor_schema_id(self):
        return self.monitor_schema.schema_id

    @cached_property
    def monitor_topic(self):
        return str(self.monitor_schema.topic.name)

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
        """Used to handle the logic of recording monitoring_message in kafka
        and resetting it if necessary.
        """
        if not self.monitoring_enabled:
            return

        self._last_msg_timestamp = message.timestamp

        track_info = self._get_record(message.topic)
        track_info = self._flush_previous_track_info(track_info)
        track_info['message_count'] += 1

    def _flush_previous_track_info(self, current_track_info):
        next_start_time = (current_track_info['start_timestamp'] +
                           self._monitoring_window_in_sec)
        while next_start_time <= self._last_msg_timestamp:
            self._publish(current_track_info)
            self._reset_monitoring_record(current_track_info)
            next_start_time = (current_track_info['start_timestamp'] +
                               self._monitoring_window_in_sec)
        return current_track_info

    def flush_buffered_info(self):
        """Publishes the buffered information, stored in topic_to_tracking_info_map,
        to kafka and resets topic_to_tracking_info_map to an empty dictionary
        """
        if not self.monitoring_enabled:
            return

        for track_info in self.topic_to_tracking_info_map.values():
            last_track_info = self._flush_previous_track_info(track_info)
            self._publish(last_track_info)
        self.producer.flush_buffered_messages()
        self.topic_to_tracking_info_map = {}

    def close(self):
        """Called when the associated client is exiting/closing. Calls flush_buffered_info
        and also closes monitoring.producer.
        """
        if not self.monitoring_enabled:
            return

        self.flush_buffered_info()
        self.producer.close()
