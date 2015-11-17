# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import staticconf
from cached_property import cached_property
from kafka import KafkaClient
from swaggerpy import client
from yelp_kafka.config import ClusterConfig
from yelp_kafka.discovery import get_cluster_by_name
from yelp_servlib.config_util import get_service_host_and_port


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
    """
    YOCALHOST = '169.254.255.254'

    @cached_property
    def logger(self):
        """Logger instance for the clientlib"""
        return logging.getLogger('data_pipeline_clientlib')

    @property
    def kafka_client(self):
        """Handles building a Kafka connection.  By default, this will connect to
        the Kafka instance in the included docker-compose file.
        """
        return KafkaClient(self.cluster_config.broker_list)

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
        if self.load_schematizer_host_and_port_from_smartstack:
            host, port = get_service_host_and_port('schematizer.main')
            return "{0}:{1}".format(host, port)
        else:
            return data_pipeline_conf.read_string(
                'schematizer_host_and_port',
                default='{0}:49256'.format(self.YOCALHOST)
            )

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
        """Returns a swagger-py client for the schematizer api.

        By default, this will connect to a schematizer instance running in the
        included docker-compose file.
        """
        return client.get_client(
            'http://{0}/api-docs'.format(self.schematizer_host_and_port)
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
        if self.kafka_cluster_type is not None and self.kafka_cluster_name is not None:
            return get_cluster_by_name(self.kafka_cluster_type, self.kafka_cluster_name)
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
        e.g., 'AES_MODE_CBC-1'"""
        return data_pipeline_conf.read_string(
            'encryption_type',
            default=None
        )

    @property
    def key_location(self):
        """Directory in which to look for key to encrypt pii."""
        return data_pipeline_conf.read_string(
            'key_location',
            default='/nail/srv/configs/data_pipeline/'
        )

    @property
    def data_pipeline_teams_config_file_path(self):
        """Returns the path to the config file which specifies valid teams for
        the data pipeline.
        """
        return data_pipeline_conf.read_string(
            'data_pipeline_teams_config_file_path',
            default='/nail/etc/services/data_pipeline/teams.yaml'
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
