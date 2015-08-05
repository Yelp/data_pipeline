# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import pytest
import staticconf

from data_pipeline.config import configure_from_dict
from data_pipeline.config import get_config
from data_pipeline.config import namespace


class TestConfigBase(object):
    @pytest.fixture
    def config(self):
        return get_config()


class TestConfigDefaults(TestConfigBase):
    @pytest.fixture
    def yocalhost(self):
        return "169.254.255.254"

    @pytest.fixture
    def broker_list(self, yocalhost):
        return ["{0}:49255".format(yocalhost)]

    @pytest.fixture
    def zookeeper(self, yocalhost):
        return "{0}:32796".format(yocalhost)

    def test_schmatizer_host_and_port(self, config, yocalhost):
        assert config.schematizer_host_and_port == "{0}:49256".format(yocalhost)

    def test_load_schematizer_host_and_port_from_smartstack(self, config):
        assert not config.load_schematizer_host_and_port_from_smartstack

    def test_cluster_config(self, config, broker_list, zookeeper):
        cluster_config = config.cluster_config
        assert cluster_config.broker_list == broker_list
        assert cluster_config.zookeeper == zookeeper

    def test_kafka_cluster_type(self, config):
        assert config.kafka_cluster_type is None

    def test_kafka_cluster_name(self, config):
        assert config.kafka_cluster_name is None

    def test_kafka_broker_list(self, config, broker_list):
        assert config.kafka_broker_list == broker_list

    def test_kafka_zookeeper(self, config, zookeeper):
        assert config.kafka_zookeeper == zookeeper

    def test_consumer_max_buffer_size_default(self, config):
        assert config.consumer_max_buffer_size_default == 1000

    def test_consumer_get_messages_timeout_default(self, config):
        assert config.consumer_get_messages_timeout_default == 0.1

    def test_consumer_partitioner_cooldown_default(self, config):
        assert config.consumer_partitioner_cooldown_default == 0.5

    def test_consumer_worker_min_sleep_time_default(self, config):
        assert config.consumer_worker_min_sleep_time_default == 0.1

    def test_consumer_worker_max_sleep_time_default(self, config):
        assert config.consumer_worker_max_sleep_time_default == 0.2

    def test_monitoring_window_in_sec(self, config):
        assert config.monitoring_window_in_sec == 600

    def test_topic_creation_wait_timeout(self, config):
        assert config.topic_creation_wait_timeout == 60

    def test_skip_messages_with_pii(self, config):
        assert config.skip_messages_with_pii


class TestConfigurationOverrides(TestConfigBase):
    @classmethod
    def setup_class(cls):
        """SmartStack needs to be configured to use service discovery, so
        doing that for the entire class.
        """
        staticconf.YamlConfiguration(
            '/nail/etc/services/services.yaml',
            namespace='smartstack_services'
        )

    @pytest.fixture
    def addr(self):
        return 'testing:1234'

    @pytest.fixture
    def cluster_name(self):
        return 'uswest2-devc'

    @pytest.fixture
    def cluster_type(self):
        return 'datapipe'

    @contextmanager
    def reconfigure(self, **kwargs):
        conf_namespace = staticconf.config.get_namespace(namespace)
        starting_config = conf_namespace.get_config_values()
        configure_from_dict(kwargs)
        try:
            yield
        finally:
            staticconf.config.get_namespace(namespace).clear()
            configure_from_dict(starting_config)

    def test_schematizer_host_and_port(self, config, addr):
        with self.reconfigure(schematizer_host_and_port=addr):
            assert config.schematizer_host_and_port == addr

    def test_load_schematizer_host_and_port_from_smartstack(self, config):
        with self.reconfigure(load_schematizer_host_and_port_from_smartstack=True):
            assert config.schematizer_host_and_port == '169.254.255.254:20912'

    def test_kafka_discovery(self, config, cluster_name, cluster_type):
        with self.reconfigure(
            kafka_cluster_type=cluster_type,
            kafka_cluster_name=cluster_name
        ):
            cluster_config = config.cluster_config
            assert cluster_config.name == cluster_name

    def test_kafka_discovery_precedence(self, config, addr, cluster_name, cluster_type):
        with self.reconfigure(
            kafka_cluster_type=cluster_type,
            kafka_cluster_name=cluster_name,
            kafka_broker_list=[addr],
            kafka_zookeeper=addr
        ):
            cluster_config = config.cluster_config
            assert cluster_config.name == cluster_name
            assert cluster_config.broker_list != [addr]
            assert cluster_config.zookeeper != addr

    def test_kafka_broker_list(self, config, addr):
        with self.reconfigure(kafka_broker_list=[addr]):
            assert config.cluster_config.broker_list == [addr]

    def test_kafka_zookeeper(self, config, addr):
        with self.reconfigure(kafka_zookeeper=addr):
            assert config.cluster_config.zookeeper == addr

    def test_consumer_max_buffer_size_default(self, config):
        with self.reconfigure(consumer_max_buffer_size_default=10):
            assert config.consumer_max_buffer_size_default == 10

    def test_consumer_get_messages_timeout_default(self, config):
        with self.reconfigure(consumer_get_messages_timeout_default=10.0):
            assert config.consumer_get_messages_timeout_default == 10.0

    def test_consumer_partitioner_cooldown_default(self, config):
        with self.reconfigure(consumer_partitioner_cooldown_default=10.0):
            assert config.consumer_partitioner_cooldown_default == 10.0

    def test_consumer_worker_min_sleep_time_default(self, config):
        with self.reconfigure(consumer_worker_min_sleep_time_default=10.0):
            assert config.consumer_worker_min_sleep_time_default == 10.0

    def test_consumer_worker_max_sleep_time_default(self, config):
        with self.reconfigure(consumer_worker_max_sleep_time_default=10.0):
            assert config.consumer_worker_max_sleep_time_default == 10.0

    def test_monitoring_window_in_sec(self, config):
        with self.reconfigure(monitoring_window_in_sec=10):
            assert config.monitoring_window_in_sec == 10

    def test_topic_creation_wait_timeout(self, config):
        with self.reconfigure(topic_creation_wait_timeout=10):
            assert config.topic_creation_wait_timeout == 10

    def test_skip_messages_with_pii(self, config):
        with self.reconfigure(skip_messages_with_pii=False):
            assert not config.skip_messages_with_pii
