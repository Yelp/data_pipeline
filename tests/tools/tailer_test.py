# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import namedtuple

import kafka_utils
import mock
import pytest
from yelp_batch.batch import BatchOptionParser

import data_pipeline
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import CreateMessage
from data_pipeline.message import RefreshMessage
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
    def dp_tailer(self):
        return Tailer()

    @pytest.yield_fixture
    def mock_exit(self, dp_tailer):
        with mock.patch.object(BatchOptionParser, 'exit') as mock_exit:
            yield mock_exit

    @pytest.fixture(scope='module')
    def topic_name(self, registered_schema, containers):
        _topic_name = str(registered_schema.topic.name)
        containers.create_kafka_topic(_topic_name)
        return _topic_name

    @pytest.fixture(scope='module')
    def topic_two_name(self, pii_schema, containers):
        _topic_name = str(pii_schema.topic.name)
        containers.create_kafka_topic(_topic_name)
        return _topic_name

    @pytest.fixture
    def topics(self, topic_name, topic_two_name, source):
        return [Topic(
            topic_id='10',
            name=topic_name,
            source=source,
            contains_pii=False,
            cluster_type='datapipe',
            primary_keys=[],
            created_at=time.time(),
            updated_at=time.time()
        ), Topic(
            topic_id='11',
            name=topic_two_name,
            source=source,
            contains_pii=False,
            cluster_type='datapipe',
            primary_keys=[],
            created_at=time.time() + 10000,
            updated_at=time.time() + 10000
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
    def producer(self, team_name):
        instance = Producer(
            producer_name="tailer_producer",
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=False,
            monitoring_enabled=False
        )
        with instance as producer:
            yield producer

    @pytest.fixture
    def default_fields(self):
        return ['payload_data']

    @pytest.fixture
    def message(self, registered_schema):
        return CreateMessage(
            registered_schema.schema_id,
            payload_data={'good_field': 100},
            timestamp=1500
        )

    @pytest.fixture
    def refresh_message(self, registered_schema):
        return RefreshMessage(
            registered_schema.schema_id,
            payload_data={'good_field': 200},
            timestamp=1200
        )

    @pytest.fixture
    def topic_with_refresh_message(self, refresh_message, producer):
        topic, offset = self._get_published_topic_and_offset(refresh_message, producer)
        return self._construct_topic_with_offset_arg(topic, offset - 1)

    @pytest.fixture
    def topic_with_good_offset(self, message, producer):
        topic, offset = self._get_published_topic_and_offset(message, producer)
        return self._construct_topic_with_offset_arg(topic, offset - 1)

    def _get_published_topic_and_offset(self, message, producer):
        topic_name = message.topic
        producer.publish(message)
        producer.flush()
        position_data = producer.get_checkpoint_position_data()
        offset = position_data.topic_to_kafka_offset_map[topic_name]
        return topic_name, offset

    def _construct_topic_with_offset_arg(self, topic_name, offset):
        return "{}|{}".format(topic_name, offset)

    @pytest.fixture
    def topic_with_bad_offset(self, message, producer):
        topic, offset = self._get_published_topic_and_offset(message, producer)
        return self._construct_topic_with_offset_arg(topic, offset + 1)

    def test_version(self, dp_tailer):
        assert dp_tailer.version == "data_pipeline {}".format(
            data_pipeline.__version__
        )

    def test_without_topics(self, dp_tailer, mock_exit):
        self._init_batch(dp_tailer, [])

        self._assert_no_topics_error(mock_exit)

    def test_with_explicit_topics(self, dp_tailer):
        self._init_batch(dp_tailer, ['--topic=topic1', '--topic=topic2'])
        assert set(dp_tailer.options.topics) == {'topic1', 'topic2'}

    def test_without_fields(self, dp_tailer, mock_exit, default_fields):
        self._init_batch(dp_tailer, [])
        assert set(dp_tailer.options.fields) == set(default_fields)
        self._assert_no_topics_error(mock_exit)

    def test_specify_fields(self, dp_tailer, mock_exit, default_fields):
        self._init_batch(dp_tailer, ['--field=kafka_position_info'])
        assert set(dp_tailer.options.fields) == set(
            default_fields + ['kafka_position_info']
        )
        self._assert_no_topics_error(mock_exit)

    def test_filter_by_namespace(
        self,
        dp_tailer,
        topic_name,
        topic_two_name,
        mock_get_topics_by_criteria,
        namespace
    ):
        self._init_batch(dp_tailer, ['--namespace=%s' % namespace])
        self._assert_topics(dp_tailer, [topic_name, topic_two_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            namespace=namespace
        )

    def test_filter_by_source(
        self,
        dp_tailer,
        topic_name,
        topic_two_name,
        mock_get_topics_by_criteria,
        source
    ):
        self._init_batch(dp_tailer, ['--source=%s' % source])
        self._assert_topics(dp_tailer, [topic_name, topic_two_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            source=source
        )

    def test_filter_by_namespace_and_source(
        self,
        dp_tailer,
        topic_name,
        topic_two_name,
        mock_get_topics_by_criteria,
        namespace,
        source
    ):
        self._init_batch(dp_tailer, [
            '--namespace', namespace,
            '--source', source
        ])
        self._assert_topics(dp_tailer, [topic_name, topic_two_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            namespace=namespace,
            source=source
        )

    def test_tail_newest_topic_in_namespace_and_source(
        self,
        dp_tailer,
        topic_name,
        topic_two_name,
        mock_get_topics_by_criteria,
        namespace,
        source
    ):
        self._init_batch(dp_tailer, [
            '--namespace', namespace,
            '--source', source,
            '--only-newest'
        ])
        self._assert_topics(dp_tailer, [topic_two_name])
        self._assert_get_topics_called(
            mock_get_topics_by_criteria,
            namespace=namespace,
            source=source
        )

    def test_tailing(self, dp_tailer, producer, message, topic_name, capsys):
        self._init_batch(dp_tailer, ['--topic', topic_name])

        # Only run for one iteration - and publish a message before starting
        # that iteration.
        def run_once_publishing_message(message_count):
            if message_count > 0:
                return False

            producer.publish(message)
            producer.flush()
            return True

        with mock.patch.object(
            Tailer,
            'keep_running',
            side_effect=run_once_publishing_message
        ):
            dp_tailer.run()

        out, _ = capsys.readouterr()
        assert out == "{u'payload_data': {u'good_field': 100}}\n"

    def test_offset(self, dp_tailer, topic_with_good_offset, capsys):
        self._init_batch(
            dp_tailer,
            ['--topic', topic_with_good_offset, '--message-limit', '1']
        )

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert out == "{u'payload_data': {u'good_field': 100}}\n"

    def test_offset_out_of_range_too_high(
        self,
        dp_tailer,
        topic_with_bad_offset,
        mock_exit
    ):
        self._init_batch(dp_tailer, ['--topic', topic_with_bad_offset])
        self._assert_offset_out_of_range_error(mock_exit)

    def test_offset_out_of_range_too_low(
        self,
        dp_tailer,
        mock_exit,
        topic_with_good_offset,
        topic_name
    ):
        PartitionOffsets = namedtuple(
            'PartitionOffsets',
            ['topic', 'partition', 'highmark', 'lowmark']
        )
        with mock.patch.object(
            kafka_utils.util.offsets,
            'get_topics_watermarks',
            return_value={
                topic_name: {0: PartitionOffsets(topic_name, 0, 13, 12)}
            }
        ):
            self._init_batch(dp_tailer, ['--topic', topic_with_good_offset])
        self._assert_offset_out_of_range_error(mock_exit)

    def test_json(self, dp_tailer, topic_with_good_offset, capsys):
        self._init_batch(dp_tailer, [
            '--topic', topic_with_good_offset,
            '--message-limit', '1',
            '--json'
        ])
        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert out == '{"payload_data": {"good_field": 100}}\n'

    def test_all_fields(self, dp_tailer, topic_with_good_offset, capsys):
        self._init_batch(dp_tailer, [
            '--topic', topic_with_good_offset,
            '--message-limit', '1',
            '--all-fields'
        ])

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert "{u'good_field': 100}" in out
        assert "'uuid_hex'" in out
        assert "'encryption_type'" in out
        assert "'kafka_position_info'" in out
        assert "'topic'" in out
        assert "'schema_id'" in out
        assert "'meta'" in out
        assert "'timestamp': 1500" in out
        assert "'contains_pii'" in out
        assert "'payload_diff'" in out
        assert "'message_type': MessageType.create(1)" in out

    def test_all_fields_for_refresh_message(self, dp_tailer, topic_with_refresh_message, capsys):
        self._init_batch(dp_tailer, [
            '--topic', topic_with_refresh_message,
            '--message-limit', '1',
            '--all-fields'
        ])

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert "{u'good_field': 200}" in out
        assert "'uuid_hex'" in out
        assert "'encryption_type'" in out
        assert "'kafka_position_info'" in out
        assert "'topic'" in out
        assert "'schema_id'" in out
        assert "'meta'" in out
        assert "'timestamp': 1200" in out
        assert "'contains_pii'" in out
        assert "'message_type': MessageType.refresh(4)" in out
        assert "'payload_diff'" not in out

    def test_iso_time(self, dp_tailer, topic_with_good_offset, capsys):
        self._init_batch(dp_tailer, [
            '--topic', topic_with_good_offset,
            '--message-limit', '1',
            '--field', 'timestamp',
            '--iso-time'
        ])

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert "u'timestamp': '1970-01-01T00:25:00'" in out
        assert "u'payload_data': {u'good_field': 100}" in out

    def test_bad_end_timestamp(
        self,
        dp_tailer,
        topic_with_good_offset,
        message,
        capsys
    ):
        self._init_batch(dp_tailer, [
            '--topic', topic_with_good_offset,
            '--end-timestamp', str(message.timestamp)
        ])

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert "good_field" not in out  # No message should be printed

    def test_good_end_timestamp(
        self,
        dp_tailer,
        topic_with_good_offset,
        message,
        capsys
    ):
        self._init_batch(dp_tailer, [
            '--topic', topic_with_good_offset,
            '--end-timestamp', str(message.timestamp + 1),
            '--message-limit', '1'
        ])

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert "u'payload_data': {u'good_field': 100}" in out

    def test_good_start_timestamp(
        self,
        dp_tailer,
        producer,
        topic_name,
        message,
        capsys
    ):
        self._get_published_topic_and_offset(message, producer)
        self._init_batch(dp_tailer, [
            '--topic', topic_name,
            '--start-timestamp', str(message.timestamp),
            '--message-limit', '1'
        ])

        dp_tailer.run()

        out, _ = capsys.readouterr()
        assert "u'payload_data': {u'good_field': 100}" in out

    def test_bad_start_timestamp(self, dp_tailer, producer, message, topic_name):
        topic, offset = self._get_published_topic_and_offset(message, producer)
        self._init_batch(dp_tailer, [
            '--topic', topic_name,
            '--start-timestamp', str(message.timestamp + 1)
        ])

        partition = 0
        actual = dp_tailer.topic_to_offsets_map[
            topic_name
        ].partition_offset_map[
            partition
        ]
        assert actual == offset

    def _init_batch(self, dp_tailer, batch_args):
        # Prevent loading the env config
        with mock.patch.object(
            data_pipeline.tools.tailer,
            'load_default_config',
        ) as load_default_config_mock:
            dp_tailer.process_commandline_options(batch_args)
            dp_tailer._call_configure_functions()

        (config_file, env_config_file), _ = load_default_config_mock.call_args
        assert config_file == '/nail/srv/configs/data_pipeline_tools.yaml'
        assert env_config_file is None

    def _assert_no_topics_error(self, mock_exit):
        (exit_code, error_message), _ = mock_exit.call_args
        assert exit_code == 2
        assert 'At least one topic must be specified.' in error_message

    def _assert_offset_out_of_range_error(self, mock_exit):
        (exit_code, error_message), _ = mock_exit.call_args
        assert exit_code == 2
        assert "Offset" in error_message and "is out of range" in error_message

    def _assert_get_topics_called(
        self,
        mock_get_topics_by_criteria,
        namespace=None,
        source=None
    ):
        _, kwargs = mock_get_topics_by_criteria.call_args
        assert kwargs == {'namespace_name': namespace, 'source_name': source}

    def _assert_topics(self, dp_tailer, topics):
        assert dp_tailer.topic_to_offsets_map == {topic: None for topic in topics}
