# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import mock
import pytest
from yelp_batch.batch import BatchOptionParser

import data_pipeline.tools.tailer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.models.topic import Topic
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.tailer import Tailer


@pytest.mark.usefixtures(
    "configure_teams",
    "containers"
)
class TestTailer(object):

    @pytest.fixture
    def batch(self):
        return Tailer()

    @pytest.yield_fixture
    def mock_exit(self, batch):
        with mock.patch.object(BatchOptionParser, 'exit') as mock_exit:
            yield mock_exit

    @pytest.fixture
    def topics(self, topic_name, source):
        return [Topic(
            topic_id='10',
            name=topic_name,
            source=source,
            contains_pii=False,
            created_at=time.time(),
            updated_at=time.time()
        )]

    @pytest.yield_fixture
    def mock_get_topics_by_criteria(self, topics):
        with mock.patch.object(
            get_schematizer(),
            'get_topics_by_criteria',
            return_value=topics,
            autospec=True
        ) as mock_schematizer:
            yield mock_schematizer

    @pytest.yield_fixture
    def producer(self, kafka_docker, team_name):
        instance = Producer(
            producer_name="tailer_producer",
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False,
            monitoring_enabled=False
        )
        with instance as producer:
            yield producer

    def test_version(self, batch):
        assert batch.version == "data_pipeline {}".format(
            data_pipeline.__version__
        )

    def test_without_topics(self, batch, mock_exit):
        self._init_batch(batch, [])
        self._assert_no_topics_error(mock_exit)

    def test_with_explicit_topics(self, batch):
        self._init_batch(batch, ['--topic=topic1', '--topic=topic2'])
        assert set(batch.options.topics) == set(['topic1', 'topic2'])

    def test_with_namespace(
        self,
        batch,
        topic_name,
        mock_get_topics_by_criteria,
        namespace
    ):
        self._init_batch(batch, ['--namespace=%s' % namespace])
        self._assert_topics(batch, [topic_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            namespace=namespace
        )

    def test_with_source(
        self,
        batch,
        topic_name,
        mock_get_topics_by_criteria,
        source
    ):
        self._init_batch(batch, ['--source=%s' % source])
        self._assert_topics(batch, [topic_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            source=source
        )

    def test_with_namespace_and_source(
        self,
        batch,
        topic_name,
        mock_get_topics_by_criteria,
        namespace,
        source
    ):
        self._init_batch(batch, [
            '--namespace', namespace,
            '--source', source
        ])
        self._assert_topics(batch, [topic_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            namespace=namespace,
            source=source
        )

    def test_tailing(
        self,
        batch,
        producer,
        message_with_payload_data,
        topic,
        capsys
    ):
        self._init_batch(batch, ['--topic', topic])

        # Only run for one iteration - and publish a message before starting
        # that iteration.
        def run_once_publishing_message(message_count):
            if message_count > 0:
                return False

            producer.publish(message_with_payload_data)
            producer.flush()
            return True

        with mock.patch.object(
            Tailer,
            'keep_running',
            side_effect=run_once_publishing_message
        ):
            batch.run()

        out, _ = capsys.readouterr()
        assert out == "{u'good_field': 100}\n"

    def test_with_offset(
        self,
        batch,
        producer,
        message_with_payload_data,
        topic,
        capsys
    ):
        message_with_payload_data.payload_data['good_field'] = 42
        topic_with_offset = self._publish_and_set_topic_offset(
            message_with_payload_data,
            producer,
            topic
        )
        self._init_batch(batch, ['--topic', topic_with_offset, '--message-limit', '1'])

        batch.run()

        out, _ = capsys.readouterr()
        assert out == "{u'good_field': 42}\n"

    def test_with_envelope_data(
        self,
        batch,
        producer,
        message_with_payload_data,
        topic,
        capsys
    ):
        topic_with_offset = self._publish_and_set_topic_offset(
            message_with_payload_data,
            producer,
            topic
        )
        self._init_batch(batch, [
            '--topic', topic_with_offset,
            '--message-limit', '1',
            '--include-envelope-data'
        ])

        batch.run()

        out, _ = capsys.readouterr()
        assert "{u'good_field': 100}" in out
        assert "'uuid'" in out
        assert "'timestamp': 1500" in out
        assert "'message_type': 'create'" in out

    def _init_batch(self, batch, batch_args):
        # Prevent loading the env config
        with mock.patch.object(
            data_pipeline.tools.tailer,
            'load_default_config',
        ) as load_default_config_mock:
            batch.process_commandline_options(batch_args)
            batch._call_configure_functions()

        (config_file, env_config_file), _ = load_default_config_mock.call_args
        assert config_file == '/nail/srv/configs/data_pipeline_tools.yaml'
        assert env_config_file is None

    def _assert_no_topics_error(self, mock_exit):
        (exit_code, error_message), _ = mock_exit.call_args
        assert exit_code == 2
        assert 'At least one topic must be specified.' in error_message

    def _assert_get_topics_called(
        self,
        mock_get_topics_by_criteria,
        namespace=None,
        source=None
    ):
        _, kwargs = mock_get_topics_by_criteria.call_args
        assert kwargs == {'namespace_name': namespace, 'source_name': source}

    def _assert_topics(self, batch, topics):
        assert batch.topic_to_offsets_map == {topic: None for topic in topics}

    def _publish_and_set_topic_offset(self, message, producer, topic):
        producer.publish(message)
        producer.flush()
        position_data = producer.get_checkpoint_position_data()
        offset = position_data.topic_to_kafka_offset_map[topic]
        topic_with_offset = "{}|{}".format(topic, offset - 1)
        return topic_with_offset
