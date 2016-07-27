# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import threading

import simplejson

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer

logger = get_config().logger


class Registrar(object):
    """This class holds the main functionality for Producer/Consumer registration

    Currently, the registrar holds an internal mapping of schema_ids to the last time
    they are used by the Client's subclass. Client will provide functionality for updating
    this mapping as it sends/receives messages. Every time the given threshold time is
    reached, the Client will send a serialized message using clog.

    Args:
        client_name (str): name of the associated client.
        client_type (str): type of the client the _Registrar is associated to.
            Could be either producer or consumer.
        threshold (int): The amount of time that should elapse in between the client sending
            registration messages (seconds).
    """
    # Default period for sending registration msg is 10 min
    DEFAULT_REGISTRATION_THRESHOLD_SECONDS = 600

    def __init__(
        self,
        client_name,
        client_type,
        threshold=DEFAULT_REGISTRATION_THRESHOLD_SECONDS
    ):
        self.client_name = client_name
        self.client_type = client_type
        self.threshold = threshold
        self.send_messages = False

        self.schema_to_last_seen_time_map = {}

    def publish_registration_messages(self):
        # TODO([DATAPIPE-1192|mkohli]): Send registration messages
        pass

    def registration_schema(self):
        schema_json = self._registration_schema()
        return get_schematizer().register_schema(
            namespace=schema_json['namespace'],
            source=schema_json['name'],
            schema_str=simplejson.dumps(schema_json),
            source_owner_email='bam+data_pipeline@yelp.com',
            contains_pii=False
        )

    def _registration_schema(self):
        schema_file = os.path.join(
            os.path.dirname(__file__),
            'schemas/registration_message_v1.avsc'
        )
        with open(schema_file, 'r') as f:
            schema_string = f.read()
        return simplejson.loads(schema_string)

    def register_tracked_schema_ids(self, schema_id_list):
        """This function is used to specify the lsit of avro schema IDs that this Client
            will use. When called it, it will reset the information about when each schema ID
            in schema_id_list was used last.

        Args:
            schema_id_list (list[int]): List of the schema IDs that the client will use.
        """
        for schema_id in schema_id_list:
            self.schema_to_last_seen_time_map[schema_id] = None
        # TODO([DATAPIPE-1192|mkohli]): Send registration message

    def update_schema_last_used_timestamp(self, schema_id, timestamp):
        """
        This function updates the last time that the given schema_id was used to value
        timestamp if the given timestamp occurred more recently than the last time the
        schema_id was used.

        Usage:
            This function is called by the Client subclass whenever it receives a message.

        Args:
            schema_id (int): Schema IDs of the message the Client received.
            timestamp (long): The utc time of the message that the Client received.
        """

        current_timestamp = self.schema_to_last_seen_time_map.get(schema_id)
        if current_timestamp is None or timestamp > current_timestamp:
            self.schema_to_last_seen_time_map[schema_id] = timestamp

    def start(self):
        """Start periodically sending registration messages"""
        if not self.send_messages:
            self.send_messages = True
            self._wake()

    def stop(self):
        """Force Client to stop periodically sending registration messages"""
        self.send_messages = False

    def _wake(self):
        """This class periodically sends registration messages using Clog"""
        if self.send_messages:
            self.publish_registration_messages()
            # The purpose of the Timer is for _wake to ensure it is called
            # every self.threshold amount of seconds until self.send_messages is False
            threading.Timer(self.threshold, self._wake).start()
