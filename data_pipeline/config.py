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

import logging
import os

import staticconf
from bravado.client import SwaggerClient
from cached_property import cached_property
from kafka_utils.util.config import ClusterConfig


namespace = 'data_pipeline'
data_pipeline_conf = staticconf.NamespaceReaders(namespace)


class Config(object):
    """Contains configuration data for the clientlib.

    Configuration can be adjusted using staticconf.

    Example::

        When using the clientlib in a service, the minimum recommended
        configuration is::

            module_config:
                ...
                - namespace: smartstack_services
                  file: /nail/etc/services/services.yaml
                - namespace: data_pipeline
                  config:
                    load_schematizer_host_and_port_from_smartstack: True
                    kafka_cluster_type: datapipe
                    kafka_cluster_name: uswest2-devc
                    skip_messages_with_pii: True
                - namespace: clog
                  file: /nail/srv/configs/clog.yaml
    """
    YOCALHOST = '169.254.255.254'

    @cached_property
    def logger(self):
        """Logger instance for the clientlib"""
        return logging.getLogger('data_pipeline_clientlib')

    @property
    def should_use_testing_containers(self):
        """Used as a config that will not be overwritten in tests where
        something like load_schematizer_host_and_port_from_smartstack may be.

        Overwrite this configuration for testing purposes only.

        You may also set this configuration manually when starting a test run
        similarly to how it's done in testing_helpers.containers
        """
        return data_pipeline_conf.read_string(
            'should_use_testing_containers',
            default=None
        )

    @property
    def schematizer_port(self):
        """Port for the schematizer as an int.

        If :meth:`load_schematizer_host_and_port_from_smartstack` is True, this
        value will be loaded from smartstack instead of read directly.
        """
        return int(self.schematizer_host_and_port.split(':')[-1])

    @property
    def schematizer_host_and_port(self):
        """Host and port for the schematizer, in the format `host:port`.


        If :meth:`load_schematizer_host_and_port_from_smartstack` is True, this
        value will be loaded from smartstack instead of read directly.
        """
        if (self.load_schematizer_host_and_port_from_smartstack and
                not self.should_use_testing_containers):
            from yelp_servlib.config_util import get_service_host_and_port  # NOQA
            host, port = get_service_host_and_port('schematizer.main')
            return "{0}:{1}".format(host, port)
        else:
            return data_pipeline_conf.read_string(
                'schematizer_host_and_port',
                default='{0}:49256'.format(self.YOCALHOST)
            )

    @property
    def topic_refresh_frequency_seconds(self):
        """The frequency how often the Consumer refreshes the consumer source
        topics from the Schematizer.  The frequency is specified in seconds,
        default to 300 seconds (5 minutes).  The Consumer will automatically
        start consuming messages from newly picked-up topics.
        """
        return data_pipeline_conf.read_float('topic_refresh_frequency_seconds', 300)

    @property
    def load_schematizer_host_and_port_from_smartstack(self):
        """Load the host and port from SmartStack instead of setting it
        directly.

        SmartStack must be configured with staticconf in order to use this
        option.  For information on doing that, see the `Accessing a service
        using SmartStack` section of http://y/smartstack.
        """
        return data_pipeline_conf.read_bool(
            'load_schematizer_host_and_port_from_smartstack',
            default=False
        )

    @property
    def schematizer_client(self):
        """Returns a bravado client for the schematizer api.

        By default, this will connect to a schematizer instance running in the
        included docker-compose file.
        """
        return SwaggerClient.from_url(
            'http://{0}/swagger.json'.format(self.schematizer_host_and_port)
        )

    @property
    def schematizer_client_max_connection_retry(self):
        """Maximum number of times schematizer_clientlib tries to connect
        to schematizer before giving up.
        """
        return data_pipeline_conf.read_float(
            'schematizer_client_max_connection_retry',
            default=5
        )

    @property
    def cluster_config(self):
        """Returns a yelp_kafka.config.ClusterConfig.

        This method will use :meth:`kafka_cluster_type` and
        :meth:`kafka_cluster_name` to fetch a `ClusterConfig` using Yelp's
        kafka discovery mechanism.  If they both aren't specified, it will
        fall back to creating a `ClusterConfig` from :meth:`kafka_broker_list`
        and :meth:`kafka_zookeeper`.  The default `ClusterConfig` will point
        at the testing docker container.
        """
        if (
            self.kafka_cluster_type is not None and
            self.kafka_cluster_name is not None and
            not self.should_use_testing_containers
        ):
            from yelp_kafka.discovery import get_kafka_cluster  # NOQA
            return get_kafka_cluster(self.kafka_cluster_type,
                                     'data_pipeline-client',
                                     self.kafka_cluster_name
                                     )
        else:
            return ClusterConfig(
                type='standard',
                name='data_pipeline',
                broker_list=self.kafka_broker_list,
                zookeeper=self.kafka_zookeeper
            )

    @property
    def kafka_cluster_type(self):
        """Cluster type corresponds to a file name in
        `/nail/etc/kafka_discovery` (ex. 'scribe' or 'standard' or 'datapipe').
        If you're setting this manually, `datapipe` is probably the cluster type
        you want.  See http://y/kafka_discovery for discovery details.
        """
        return data_pipeline_conf.read_string(
            'kafka_cluster_type',
            default=None
        )

    @property
    def kafka_cluster_name(self):
        """Cluster name corresponds to one of the named clusters in the cluster
        list in the discovery file for a cluster type.  For example,
        `/nail/etc/kafka_discovery/standard.yaml` clusters list contains a
        cluster `uswest1-devc`.  To use this cluster, :meth:`kafka_cluster_type`
        should be set to `standard`, and :meth:`kafka_cluster_name` should be
        set to `uswest1-devc`.
        """
        return data_pipeline_conf.read_string(
            'kafka_cluster_name',
            default=None
        )

    @property
    def kafka_broker_list(self):
        """:meth:`cluster_config` will use this list to construct a Kafka
        `ClusterConfig` in the absence of :meth:`kafka_cluster_type` or
        :meth:`kafka_cluster_name`.

        Each broker is expected to be a string in `host:port` form.
        """
        return data_pipeline_conf.read_list(
            'kafka_broker_list',
            default=['{0}:49255'.format(self.YOCALHOST)]
        )

    @property
    def kafka_zookeeper(self):
        """Zookeeper connection string - see kafka discovery files for addition
        detail.
        """
        return data_pipeline_conf.read_string(
            'kafka_zookeeper',
            default='{0}:32796'.format(self.YOCALHOST)
        )

    @property
    def zookeeper_discovery_path(self):
        return data_pipeline_conf.read_string(
            'zookeeper_discovery_path',
            default='zookeeper_discovery{ecosystem}.yaml'
        )

    @property
    def consumer_get_messages_timeout_default(self):
        """ Default timeout for blocking calls to ``Consumer.get_messages``
        """
        return data_pipeline_conf.read_float(
            'consumer_get_messages_timeout_default',
            default=0.1,
        )

    @property
    def consumer_partitioner_cooldown_default(self):
        """ Default partitioner cooldown time. See ``yelp_kafka.partitioner`` for
        more details.
        """
        return data_pipeline_conf.read_float(
            'consumer_partitioner_cooldown_default',
            default=0.5,
        )

    @property
    def consumer_use_group_sha_default(self):
        """Default use group sha option. See yelp_kafka.config for more details
        """
        return data_pipeline_conf.read_bool(
            'consumer_use_group_sha_default',
            default=True
        )

    @property
    def monitoring_window_in_sec(self):
        """Returns the duration(in sec) for which the monitoring system will count
        the number of messages processed by the client
        """
        return data_pipeline_conf.read_int(
            'monitoring_window_in_sec',
            default=600,
        )

    @property
    def topic_creation_wait_timeout(self):
        """Maximum time in seconds to wait for a kafka topic to be created
        during tests.
        """
        return data_pipeline_conf.read_int(
            'topic_creation_wait_timeout',
            default=60,
        )

    @property
    def skip_messages_with_pii(self):
        """Return true if we drop the message containing pii in kafka publish.
        """
        return data_pipeline_conf.read_bool(
            'skip_messages_with_pii',
            default=True
        )

    @property
    def encryption_type(self):
        """Algorithm and key to use when encrypting pii,
        e.g., 'AES_MODE_CBC-1'. The default here is None,
        but in the default data_pipeline config file,
        it is set to AES_MODE_CBC-1 for ease of set-up."""
        return data_pipeline_conf.read_string(
            'encryption_type',
            default=None
        )

    @property
    def key_location(self):
        """Directory in which to look for key to encrypt pii. ./key-1.key is meant for
        unit tests and documentation purpose, please use a different key in production."""
        return data_pipeline_conf.read_string(
            'key_location',
            default='./'
        )

    @property
    def data_pipeline_teams_config_file_path(self):
        """Returns the path to the config file which specifies valid teams for
        the data pipeline.
        """
        return data_pipeline_conf.read_string(
            'data_pipeline_teams_config_file_path',
            default='teams.yaml'
        )

    @property
    def kafka_client_ack_count(self):
        """ack setting of the kafka client for publishing messages.  Default to
        -1, which indicates that the server will wait until the message is
        committed by all in-sync replicas before sending a response.  See
        https://github.com/mumrah/kafka-python/blob/master/kafka/client.py#L445
        for detail information.
        """
        return data_pipeline_conf.read_int('kafka_client_ack_count', default=-1)

    @property
    def producer_max_publish_retry_count(self):
        """Number of times the producer will retry to publish messages.
        """
        return data_pipeline_conf.read_int(
            'producer_max_publish_retry_count',
            default=5
        )

    @property
    def consumer_max_offset_retry_count(self):
        """Number of times the consumer will retry to set its offsets.
        """
        return data_pipeline_conf.read_int(
            'consumer_max_offset_retry_count',
            default=5
        )

    @property
    def kafka_producer_buffer_size(self):
        """The maximum number of messages that the clientlib will buffer
        before sending them out to kafka.
        """
        return data_pipeline_conf.read_int(
            'kafka_producer_buffer_size',
            default=5000
        )

    @property
    def kafka_producer_flush_time_limit_seconds(self):
        """The maximum amount of time in seconds that the clientlib will wait
        for its buffer to fill before forcing a flush to kafka.
        """
        return data_pipeline_conf.read_float(
            'kafka_producer_flush_time_limit_seconds',
            default=0.1
        )

    @property
    def skip_position_info_update_when_not_set(self):
        """By default, the clientlib will replace upstream position info in the
        producer with the contents from the last published message, regardless
        of content.  When this option is True, the producer will not update
        position info when it's not set in the message.

        This option is useful when a single upstream message can potentially
        result in multiple downstream messages being produced.  In that case,
        only the last message should carry upstream position information.
        """
        return data_pipeline_conf.read_bool(
            'skip_position_info_update_when_not_set',
            default=False
        )

    @property
    def merge_position_info_update(self):
        """By default, the clientlib will replace upstream position info
        returned by the producer.  When this option is set to True, the
        producer will instead merge the upstream position info dictionaries.
        When used, `upstream_position_info` should always be set, or
        :meth:`skip_position_info_update_when_not_set` should be set to True.

        This can be useful if consumption happens from multiple
        upstream sources (such as a partitioned upstream topic), and
        the producer needs to merge information for each of the upstreams.
        """
        return data_pipeline_conf.read_bool(
            'merge_position_info_update',
            default=False
        )

    @property
    def force_recovery_from_publication_unensurable_error(self):
        """Toggling this option to true and restarting the impacted service will
        force the `Producer` to recover from `PublicationUnensurableError`
        generated in the `ensure_messages_published` method.  This option should
        be turned off after the service recovers.  Ideally it would never be
        used, but it can be helpful to get an exactly-once service working again
        after a logical error, at the expense of violating the guarantee.  It's
        primarily here to get services running again, while we're working out
        their kinks and debugging.  Don't use it unless you're absolutely sure
        of what you're doing.

        Warning:
            Setting this option to True guarantees that the exactly-once
            guarantee will be broken.
        """
        return data_pipeline_conf.read_bool(
            'force_recovery_from_publication_unensurable_error',
            default=False
        )

    @property
    def sensu_ping_window_seconds(self):
        """The ping window defines the minimum time (in seconds) the producer will wait
        prior to sending another OK message to sensu.  For example, say the ping window
        is 30 seconds, and an event is published at time=0.  No events prior to time=30s
        will cause the producer to publish an OK message to sensu.  Once an event comes
        in after time=30s the clock will be reset.  That is, if the next event comes in
        at time=40s the producer will send the OK, and not publish OK's again until
        after time=70s.  The purpose of this is to throtte the number events each
        producer sends to sensu.
        """
        return data_pipeline_conf.read_int('sensu_ping_window_seconds', default=30)

    @property
    def expected_heartbeat_interval(self):
        """The producer expects the upstream to send heartbeats at intervels with a maximum
        of this number of seconds.  For example, if this returns 300 then we expect the
        upstream to create at least one event every 300 seconds.
        """
        return data_pipeline_conf.read_int('expected_heartbeat_interval', default=300)

    @property
    def sensu_ttl(self):
        """This is the time to live (TTL) for the producer.  If sensu doesn't get an OK
        message by the time to live interval sensu will alert.  Note that the TTL is derived
        from the expected heartbeat and the ping window to prevent false alarms.  For example
        if the ping window is 30 seconds and the hearbeat interval is 300 seconds the TTL
        will be 331 seconds, which guarantees if things are working correctly we'll have be
        sending OK's to sensu on time.  If either the producer goes down or the upstream
        stops sending heartbeats to the producer the team owning the producer will be alerted
        within one TTL period.
        """
        return self.expected_heartbeat_interval + self.sensu_ping_window_seconds + 1

    @property
    def sensu_host(self):
        """If we're running in Paasta, use the paasta cluster from the
        environment directly as laid out in PAASTA-1579.  This makes it so that
        local-run and real sensu alerts go to the same cluster, which should
        prevent false alerts that never resolve when we run locally.
        """
        if os.environ.get('PAASTA_CLUSTER'):
            return "paasta-{cluster}.yelp".format(
                cluster=os.environ.get('PAASTA_CLUSTER')
            )
        else:
            return data_pipeline_conf.read_string('sensu_host', self.YOCALHOST)

    @property
    def sensu_page_on_critical(self):
        """If this is true the client will page on a sensu critical alert"""
        return data_pipeline_conf.read_bool('sensu_page_on_critical', default=False)

    @property
    def container_name(self):
        """This is the name of the paasta instance in which the client is running.
        For example: canary"""
        return os.environ.get(
            'PAASTA_INSTANCE',
            data_pipeline_conf.read_string('container_name', "no_paasta_container")
        )

    @property
    def container_env(self):
        """This is the name of the paasta cluster in which the client is running.
        For example: norcal-prod.
        """
        return os.environ.get(
            'PAASTA_CLUSTER',
            data_pipeline_conf.read_string('container_env', "no_paasta_environment")
        )

    @property
    def sensu_source(self):
        """This ensures that the alert tracks both the paasta environment and
        the running instance, so we can have separate alerts for the canary
        and the main instances.
        """
        return '{container_env}_{container_name}'.format(
            container_env=self.container_env,
            container_name=self.container_name
        )

    @property
    def sensu_alert_after_seconds(self):
        """This ensures that the alert tracks both the paasta environment and
        the running instance, so we can have separate alerts for the canary
        and the main instances.
        """
        return "{}s".format(
            data_pipeline_conf.read_int(
                'sensu_alert_after_seconds',
                default=300
            )
        )

    @property
    def enable_sensu(self):
        """enable sensu alerting"""
        return data_pipeline_conf.read_bool('enable_sensu', False)

    @property
    def enable_meteorite(self):
        """enable meteorite monitoring"""
        return data_pipeline_conf.read_bool('enable_meteorite', False)

    @property
    def max_producer_delay_seconds(self):
        """This is the maximum number of seconds allowed between the event times
        and the time the producer is to publish the message to kafka.  Anything
        greater than this time will produce an alert"""
        return data_pipeline_conf.read_int('sensu_max_delay_seconds', 300)

    @property
    def ecosystem_file_path(self):
        """Path to the file that specifies the ecosystem."""
        return data_pipeline_conf.read_string('ecosystem_file_path', "ecosystem")


def configure_from_dict(config_dict):
    """Configure the :mod:`data_pipeline` clientlib from a dictionary.

    Args:
        config_dict (dict): a dict of config data
    """
    staticconf.DictConfiguration(config_dict, namespace=namespace)


_config = Config()


def get_config():
    """Returns the global data pipeline configuration object"""
    return _config
