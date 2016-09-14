# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import staticconf

from data_pipeline.config import get_config
from tests.helpers.config import reconfigure


class TestConfigBase(object):
    @pytest.fixture
    def config(self):
        return get_config()

    @pytest.fixture
    def yocalhost(self):
        return "169.254.255.254"


class TestConfigDefaults(TestConfigBase):
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

    def test_consumer_get_messages_timeout_default(self, config):
        assert config.consumer_get_messages_timeout_default == 0.1

    def test_consumer_partitioner_cooldown_default(self, config):
        assert config.consumer_partitioner_cooldown_default == 0.5

    def test_use_group_sha_default(self, config):
        assert config.consumer_use_group_sha_default is True

    def test_monitoring_window_in_sec(self, config):
        assert config.monitoring_window_in_sec == 600

    def test_topic_creation_wait_timeout(self, config):
        assert config.topic_creation_wait_timeout == 60

    def test_skip_messages_with_pii(self, config):
        assert config.skip_messages_with_pii

    def test_active_encryption_key(self, config):
        assert config.encryption_type is None

    def test_data_pipeline_teams_config_file_path(self, config):
        assert config.data_pipeline_teams_config_file_path == '/nail/etc/services/data_pipeline/teams.yaml'

    def test_kafka_client_ack_count(self, config):
        assert config.kafka_client_ack_count == -1

    def test_producer_max_publish_retry_count(self, config):
        assert config.producer_max_publish_retry_count == 5

    def test_kafka_producer_buffer_size(self, config):
        assert config.kafka_producer_buffer_size == 5000

    def test_kafka_producer_flush_time_limit_seconds(self, config):
        assert config.kafka_producer_flush_time_limit_seconds == 0.1

    def test_skip_position_info_update_when_not_set(self, config):
        assert not config.skip_position_info_update_when_not_set

    def test_merge_position_info_update(self, config):
        assert not config.merge_position_info_update

    def test_force_recovery_from_publication_unensurable_error(self, config):
        assert not config.force_recovery_from_publication_unensurable_error

    def test_should_use_testing_containers(self, config):
        assert not config.should_use_testing_containers


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

    def test_schematizer_host_and_port(self, config, addr):
        with reconfigure(schematizer_host_and_port=addr):
            assert config.schematizer_host_and_port == addr

    def test_load_schematizer_host_and_port_from_smartstack(self, config, yocalhost):
        with reconfigure(load_schematizer_host_and_port_from_smartstack=True):
            assert config.schematizer_host_and_port == '{0}:20912'.format(yocalhost)

    def test_kafka_discovery(self, config, cluster_name, cluster_type):
        with reconfigure(
            kafka_cluster_type=cluster_type,
            kafka_cluster_name=cluster_name
        ):
            cluster_config = config.cluster_config
            assert cluster_config.name == cluster_name

    def test_kafka_discovery_precedence(self, config, addr, cluster_name, cluster_type):
        with reconfigure(
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
        with reconfigure(kafka_broker_list=[addr]):
            assert config.cluster_config.broker_list == [addr]

    def test_kafka_zookeeper(self, config, addr):
        with reconfigure(kafka_zookeeper=addr):
            assert config.cluster_config.zookeeper == addr

    def test_consumer_get_messages_timeout_default(self, config):
        with reconfigure(consumer_get_messages_timeout_default=10.0):
            assert config.consumer_get_messages_timeout_default == 10.0

    def test_consumer_partitioner_cooldown_default(self, config):
        with reconfigure(consumer_partitioner_cooldown_default=10.0):
            assert config.consumer_partitioner_cooldown_default == 10.0

    def test_consumer_use_group_sha_default(self, config):
        with reconfigure(consumer_use_group_sha_default=False):
            assert config.consumer_use_group_sha_default is False

    def test_monitoring_window_in_sec(self, config):
        with reconfigure(monitoring_window_in_sec=10):
            assert config.monitoring_window_in_sec == 10

    def test_topic_creation_wait_timeout(self, config):
        with reconfigure(topic_creation_wait_timeout=10):
            assert config.topic_creation_wait_timeout == 10

    def test_skip_messages_with_pii(self, config):
        with reconfigure(skip_messages_with_pii=False):
            assert not config.skip_messages_with_pii

    def test_data_pipeline_teams_config_file_path(self, config):
        with reconfigure(data_pipeline_teams_config_file_path='/some/path'):
            assert config.data_pipeline_teams_config_file_path == '/some/path'

    def test_kafka_client_ack_count(self, config):
        with reconfigure(kafka_client_ack_count=1):
            assert config.kafka_client_ack_count == 1

    def test_producer_max_publish_retry_count(self, config):
        with reconfigure(producer_max_publish_retry_count=3):
            assert config.producer_max_publish_retry_count == 3

    def test_kafka_producer_buffer_size(self, config):
        with reconfigure(kafka_producer_buffer_size=10):
            assert config.kafka_producer_buffer_size == 10

    def test_kafka_producer_flush_time_limit_seconds(self, config):
        with reconfigure(kafka_producer_flush_time_limit_seconds=3.2):
            assert config.kafka_producer_flush_time_limit_seconds == 3.2

    def test_skip_position_info_update_when_not_set(self, config):
        with reconfigure(skip_position_info_update_when_not_set=True):
            assert config.skip_position_info_update_when_not_set

    def test_merge_position_info_update(self, config):
        with reconfigure(merge_position_info_update=True):
            assert config.merge_position_info_update

    def test_force_recovery_from_publication_unensurable_error(self, config):
        with reconfigure(force_recovery_from_publication_unensurable_error=True):
            assert config.force_recovery_from_publication_unensurable_error
