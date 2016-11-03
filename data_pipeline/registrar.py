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

import os
import threading

import simplejson
from cached_property import cached_property

from data_pipeline._clog_writer import ClogWriter
from data_pipeline.config import get_config
from data_pipeline.message import RegistrationMessage
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer

logger = get_config().logger


class Registrar(object):
    """This class holds the main functionality for Producer/Consumer registration

    Currently, the registrar holds an internal mapping of schema_ids to the last time
    they are used by the Client's subclass. Client will provide functionality for updating
    this mapping as it sends/receives messages. Every time the given threshold time is
    reached, the Client will send a serialized message using clog.

    Args:
        team_name (str): Team name, as defined in `sensu_handlers::teams` (see y/sensu-teams).
        client_name (str): name of the associated client.
        client_type (str): type of the client the _Registrar is associated to.
            Could be either producer or consumer.
        expected_frequency_seconds (int): How frequently, in seconds, that the client expects
            to run to produce or consume messages.
        threshold (int): The amount of time that should elapse in between the client sending
            registration messages (seconds).
    """

    # Default period for sending registration msg is 10 min
    DEFAULT_REGISTRATION_THRESHOLD_SECONDS = 600

    def __init__(
        self,
        team_name,
        client_name,
        client_type,
        expected_frequency_seconds,
        threshold=DEFAULT_REGISTRATION_THRESHOLD_SECONDS
    ):
        self.team_name = team_name
        self.client_name = client_name
        self.client_type = client_type
        self.threshold = threshold
        self.send_messages = False
        self.schema_to_last_seen_time_map = {}
        self.expected_frequency_seconds = expected_frequency_seconds
        self.clog_writer = ClogWriter()
        self.current_thread = None

    def publish_registration_messages(self):
        """
        Publish all registration messages to Scribe.
        """
        for message in self.get_registration_messages():
            self.clog_writer.publish(message)

    def get_registration_messages(self):
        """
        This function will convert the internal state of the registrar into instances of
        the RegistrationMessage class.
        """
        registration_messages = [
            self._create_registration_message(schema_id, timestamp)
            for schema_id, timestamp in self.schema_to_last_seen_time_map.iteritems()
        ]
        return registration_messages

    def _create_registration_message(self, schema_id, last_seen_timestamp):
        """
        Return single instance of a RegistrationMessage using passed in schema_id and
        timestamp.
        """
        payload_data = self._registration_message_payload(schema_id, last_seen_timestamp)
        return RegistrationMessage(
            schema_id=self.registration_schema.schema_id,
            payload_data=payload_data
        )

    def _registration_message_payload(self, schema_id, last_seen_timestamp):
        return {
            "team_name": self.team_name,
            "client_name": self.client_name,
            "client_type": self.client_type,
            "timestamp": last_seen_timestamp,
            "expected_frequency_seconds": self.expected_frequency_seconds,
            "schema_id": schema_id
        }

    @cached_property
    def registration_schema(self):
        schema_json = self._registration_schema
        return get_schematizer().register_schema(
            namespace=schema_json['namespace'],
            source=schema_json['name'],
            schema_str=simplejson.dumps(schema_json),
            source_owner_email='bam+data_pipeline@yelp.com',
            contains_pii=False
        )

    @cached_property
    def _registration_schema(self):
        schema_file = os.path.join(
            os.path.dirname(__file__),
            'schemas/registration_message_v1.avsc'
        )
        with open(schema_file, 'r') as f:
            schema_string = f.read()
        return simplejson.loads(schema_string)

    def register_tracked_schema_ids(self, schema_id_list):
        """This function is used to specify the list of avro schema IDs that this Client
            will use. When called it, it will immediately publish registration messages.

        Args:
            schema_id_list (list[int]): List of the schema IDs that the client will use.
        """
        for schema_id in schema_id_list:
            self.schema_to_last_seen_time_map[schema_id] = None
        self.publish_registration_messages()

    def update_schema_last_used_timestamp(self, schema_id, timestamp_in_milliseconds):
        """
        This function updates the last time that the given schema_id was used to value
        timestamp if the given timestamp occurred more recently than the last time the
        schema_id was used.

        Usage:
            This function is called by the Client subclass whenever it receives a message.

        Args:
            schema_id (int): Schema IDs of the message the Client received.
            timestamp_in_milliseconds (long): The utc time of the message that the Client
                                              received in milliseconds.
        """
        current_timestamp = self.schema_to_last_seen_time_map.get(schema_id)
        if current_timestamp is None or timestamp_in_milliseconds > current_timestamp:
            self.schema_to_last_seen_time_map[schema_id] = timestamp_in_milliseconds

    def start(self):
        """Start periodically sending registration messages after threshold amount of time"""
        if not self.send_messages:
            self.send_messages = True
            self.current_thread = threading.Timer(self.threshold, self._wake)
            self.current_thread.start()

    def stop(self):
        """Force Client to stop periodically sending registration messages"""
        self.send_messages = False
        if self.current_thread:
            self.current_thread.cancel()
        # Send registration messages when the Registrar is stopped
        self.publish_registration_messages()

    def _wake(self):
        """This class periodically sends registration messages using Clog"""
        if self.send_messages:
            self.publish_registration_messages()
            # The purpose of the Timer is for _wake to ensure it is called
            self.current_thread = threading.Timer(self.threshold, self._wake)
            self.current_thread.start()
