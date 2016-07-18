# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import multiprocessing
import random
import time

import mock
import pytest
import simplejson as json
from kafka.common import FailedPayloadsError
from kafka.common import ProduceRequest
from kafka.common import ProduceResponse
from yelp_avro.avro_string_reader import AvroStringReader
from yelp_avro.avro_string_writer import AvroStringWriter

import data_pipeline._clog_writer
import data_pipeline.producer
from data_pipeline._clog_writer import ClogWriter
from data_pipeline._encryption_helper import EncryptionHelper
from data_pipeline._kafka_producer import _EnvelopeAndMessage
from data_pipeline._kafka_producer import _prepare
from data_pipeline._retry_util import ExpBackoffPolicy
from data_pipeline._retry_util import MaxRetryError
from data_pipeline._retry_util import RetryPolicy
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message import create_from_offset_and_message
from data_pipeline.message import CreateMessage
from data_pipeline.message import Message
from data_pipeline.message_type import _ProtectedMessageType
from data_pipeline.meta_attribute import MetaAttribute
from data_pipeline.producer import Producer
from data_pipeline.producer import PublicationUnensurableError
from data_pipeline.testing_helpers.kafka_docker import capture_new_data_pipeline_messages
from data_pipeline.testing_helpers.kafka_docker import capture_new_messages
from data_pipeline.testing_helpers.kafka_docker import setup_capture_new_messages_consumer
from tests.helpers.config import reconfigure
from tests.helpers.mock_utils import attach_spy_on_func


class RandomException(Exception):
    pass


@pytest.mark.usefixtures(
    "configure_teams",
    "patch_monitor_init_start_time_to_now"
)
class TestProducerBase(object):

    @pytest.fixture(params=[
        True,
        False
    ])
    def use_work_pool(self, request):
        return request.param

    @pytest.fixture
    def producer_name(self):
        return 'producer_1'

    @pytest.fixture
    def producer_instance(self, containers, producer_name, use_work_pool, team_name):
        return Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            use_work_pool=use_work_pool
        )

    @pytest.yield_fixture
    def producer(self, producer_instance):
        with producer_instance as producer:
            yield producer
        assert len(multiprocessing.active_children()) == 0

    @pytest.fixture(scope="module", autouse=True)
    def topic(self, registered_schema, containers):
        topic_name = str(registered_schema.topic.name)
        containers.create_kafka_topic(topic_name)
        return topic_name

    @pytest.fixture(scope="module", autouse=True)
    def topic_with_pkey(self, registered_schema_with_pkey, containers):
        topic_name = str(registered_schema_with_pkey.topic.name)
        containers.create_kafka_topic(topic_name)
        return topic_name

    @pytest.fixture(scope="module", autouse=True)
    def pii_topic(self, pii_schema, containers):
        topic_name = str(pii_schema.topic.name)
        containers.create_kafka_topic(topic_name)
        return topic_name

    @pytest.yield_fixture
    def patch_monitor_init_start_time_to_now(self):
        with mock.patch(
            'data_pipeline.client._Monitor.get_monitor_window_start_timestamp',
            return_value=int(time.time())
        ) as patched_start_time:
            yield patched_start_time

    @pytest.fixture(scope='module')
    def create_new_schema(self, schematizer_client, example_schema):
        def _create_new_schema(source=None):
            source = source or 'test_source'
            new_schema = schematizer_client.register_schema(
                namespace='test_namespace',
                source='{}_{}'.format(source, random.random()),
                schema_str=example_schema,
                source_owner_email='test@yelp.com',
                contains_pii=False
            )
            return new_schema
        return _create_new_schema

    @pytest.fixture(scope='module')
    def another_schema(self, create_new_schema):
        return create_new_schema(source='another_source')

    @pytest.fixture
    def another_topic(self, another_schema, containers):
        topic_name = str(another_schema.topic.name)
        containers.create_kafka_topic(topic_name)
        return topic_name

    @pytest.fixture
    def create_message(self, registered_schema, payload):
        def _create_message(**overrides):
            return CreateMessage(
                schema_id=registered_schema.schema_id,
                payload=payload,
                **overrides
            )
        return _create_message

    @pytest.fixture
    def message(self, create_message):
        return create_message()


class TestClogWriter(TestProducerBase):

    def test_publish_clog(self, message):
        with mock.patch.object(
            data_pipeline._clog_writer.clog,
            'log_line',
            return_value=None
        ) as mock_log_line:
            writer = ClogWriter()
            writer.publish(message)
        assert mock_log_line.called

    def test_log_error_on_exception(self, message):
        with mock.patch.object(
            data_pipeline._clog_writer.clog,
            'log_line',
            side_effect=RandomException()
        ) as mock_log_line, mock.patch.object(
            data_pipeline._clog_writer,
            'logger'
        ) as mock_logger:
            writer = ClogWriter()
            writer.publish(message)

        call_args = "Failed to scribe message - {}".format(str(message))

        assert mock_log_line.called
        assert mock_logger.error.call_args_list[0] == mock.call(call_args)


class TestProducer(TestProducerBase):

    def test_basic_publish(self, message, producer):
        self._publish_and_assert_message(message, producer)

    def test_publish_payload_data_message(self, payload_data_message, producer):
        self._publish_and_assert_message(payload_data_message, producer)

    def _publish_and_assert_message(self, message, producer):
        messages = self._publish_message(message, producer)

        assert len(messages) == 1
        assert messages[0].payload == message.payload
        assert messages[0].schema_id == message.schema_id

    def _publish_message(self, message, producer):
        with capture_new_data_pipeline_messages(message.topic) as get_messages:
            producer.publish(message)
            producer.flush()
            return get_messages()

    def test_messages_not_duplicated(self, message, producer_instance):
        with capture_new_messages(
            message.topic
        ) as get_messages, producer_instance as producer:
            producer.publish(message)
            producer.flush()

        assert len(multiprocessing.active_children()) == 0
        assert len(get_messages()) == 1

    def test_messages_published_without_flush(self, message, producer_instance):
        with capture_new_messages(
            message.topic
        ) as get_messages, producer_instance as producer:
            producer.publish(message)

        assert len(multiprocessing.active_children()) == 0
        assert len(get_messages()) == 1

    def test_empty_starting_checkpoint_data(self, producer):
        position_data = producer.get_checkpoint_position_data()
        assert position_data.last_published_message_position_info is None
        assert position_data.topic_to_last_position_info_map == {}
        assert position_data.topic_to_kafka_offset_map == {}

    def test_child_processes_do_not_survive_an_exception(self, producer_instance, message):
        with pytest.raises(RandomException), producer_instance as producer:
            producer.publish(message)
            producer.flush()
            producer.publish(message)
            raise RandomException()
        assert len(multiprocessing.active_children()) == 0

    def test_get_position_data(self, create_message, producer):
        upstream_info = {'offset': 'fake'}
        message = create_message(upstream_position_info=upstream_info)
        with setup_capture_new_messages_consumer(message.topic) as consumer:
            producer.publish(message)
            producer.flush()
            position_data = producer.get_checkpoint_position_data()

            self._verify_position_data(position_data, upstream_info, message.topic)
            self._verify_topic_kafka_offset(
                position_data,
                message.topic,
                consumer,
                producer,
                create_message
            )

    def test_position_data_callback(self, create_message, producer_name, team_name):
        callback = mock.Mock()
        producer = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            position_data_callback=callback
        )
        upstream_info = {'offset': 'fake'}
        message = create_message(upstream_position_info=upstream_info)
        with setup_capture_new_messages_consumer(message.topic) as consumer:
            producer.publish(message)
            producer.flush()
            (position_data,), _ = callback.call_args

            self._verify_position_data(position_data, upstream_info, message.topic)
            self._verify_topic_kafka_offset(
                position_data,
                message.topic,
                consumer,
                producer,
                create_message
            )

    def _verify_position_data(self, position_data, upstream_info, topic):
        assert position_data.last_published_message_position_info == upstream_info
        assert position_data.topic_to_last_position_info_map == {topic: upstream_info}

    def _verify_topic_kafka_offset(
        self, position_data, topic, consumer, producer, create_message
    ):
        # The pointer is to the next offset where messages will be published.
        # There shouldn't be any messages there yet.
        kafka_offset = position_data.topic_to_kafka_offset_map[topic]
        consumer.seek(kafka_offset, 0)  # kafka_offset from head
        assert len(consumer.get_messages(count=10)) == 0

        # publish another message, so we can seek to it
        message = create_message(upstream_position_info={'offset': 'fake2'})
        producer.publish(message)
        producer.flush()

        # There should be a message now that we've published one
        consumer.seek(kafka_offset, 0)  # kafka_offset from head
        assert len(consumer.get_messages(count=10)) == 1

    def test_skip_publish_pii_message(self, pii_schema, payload, producer_instance):
        with reconfigure(
            encryption_type='AES_MODE_CBC-1',
            skip_messages_with_pii=True
        ), producer_instance as producer, mock.patch.object(
            data_pipeline._kafka_producer,
            'logger'
        ) as mock_logger:
            pii_message = CreateMessage(
                schema_id=pii_schema.schema_id,
                payload=payload
            )
            messages = self._publish_message(pii_message, producer)

        assert len(messages) == 0
        assert len(multiprocessing.active_children()) == 0
        call_args = (
            "Skipping a PII message - uuid hex: {}, schema_id: {}, "
            "timestamp: {}, type: {}"
        ).format(
            pii_message.uuid_hex,
            pii_message.schema_id,
            pii_message.timestamp,
            pii_message.message_type.name
        )
        assert mock_logger.info.call_args_list[0] == mock.call(call_args)

    def test_publish_pii_message(self, pii_schema, payload, producer_instance):
        with reconfigure(
            encryption_type='AES_MODE_CBC-1',
            skip_messages_with_pii=False
        ), producer_instance as producer:
            pii_message = CreateMessage(
                schema_id=pii_schema.schema_id,
                payload=payload
            )
            self._publish_and_assert_pii_message(pii_message, producer)
        assert len(multiprocessing.active_children()) == 0

    def test_publish_pii_payload_data_message(
        self, pii_schema, example_payload_data, producer_instance
    ):
        with reconfigure(
            encryption_type='AES_MODE_CBC-1',
            skip_messages_with_pii=False
        ), producer_instance as producer:
            pii_message = CreateMessage(
                schema_id=pii_schema.schema_id,
                payload_data=example_payload_data
            )
            self._publish_and_assert_pii_message(pii_message, producer)
        assert len(multiprocessing.active_children()) == 0

    def _publish_and_assert_pii_message(self, message, producer):
        with capture_new_messages(message.topic) as get_messages:
            producer.publish(message)
            producer.flush()
            offsets_and_messages = get_messages()

        assert len(offsets_and_messages) == 1

        dp_message = create_from_offset_and_message(
            offsets_and_messages[0]
        )
        assert dp_message.payload == message.payload
        assert dp_message.payload_data == message.payload_data
        assert dp_message.schema_id == message.schema_id

        unpacked_message = Envelope().unpack(offsets_and_messages[0].message.value)
        unpacked_meta_attr = unpacked_message['meta'][0]
        encryption_helper = EncryptionHelper(
            dp_message.encryption_type,
            MetaAttribute(
                unpacked_meta_attr['schema_id'],
                unpacked_meta_attr['payload']
            )
        )
        encrypted_payload = encryption_helper.encrypt_payload(message.payload)
        assert unpacked_message['payload'] == encrypted_payload

    def test_publish_message_with_no_keys(
        self,
        message,
        producer
    ):
        with capture_new_messages(message.topic) as get_messages:
            producer.publish(message)
            producer.flush()
            offsets_and_messages = get_messages()
        assert len(offsets_and_messages) == 1

        dp_message = create_from_offset_and_message(
            offsets_and_messages[0]
        )
        assert dp_message.keys == {}

    def test_publish_message_with_keys(
        self,
        message_with_pkeys,
        producer
    ):
        expected_keys_avro_json = {
            "type": "record",
            "namespace": "yelp.data_pipeline",
            "name": "primary_keys",
            "doc": "Represents primary keys present in Message payload.",
            "fields": [
                {"type":"string", "name":"field2", "doc":"test", "pkey":1},
                {"type":"int", "name":"field1", "doc":"test", "pkey":2},
                {"type":"int", "name":"field3", "doc":"test", "pkey":3},
            ]
        }
        expected_keys = {
            "field2": message_with_pkeys.payload_data["field2"],
            "field1": message_with_pkeys.payload_data["field1"],
            "field3": message_with_pkeys.payload_data["field3"]
        }

        with capture_new_messages(message_with_pkeys.topic) as get_messages:
            producer.publish(message_with_pkeys)
            producer.flush()
            offsets_and_messages = get_messages()
        assert len(offsets_and_messages) == 1

        dp_message = create_from_offset_and_message(
            offsets_and_messages[0]
        )
        assert dp_message.keys == expected_keys

        avro_string_writer = AvroStringWriter(
            schema=expected_keys_avro_json
        )
        expected_encoded_keys = avro_string_writer.encode(
            message_avro_representation=expected_keys
        )
        assert offsets_and_messages[0].message.key == expected_encoded_keys

        avro_string_reader = AvroStringReader(
            reader_schema=expected_keys_avro_json,
            writer_schema=expected_keys_avro_json
        )
        decoded_keys = avro_string_reader.decode(
            encoded_message=offsets_and_messages[0].message.key
        )
        assert decoded_keys == expected_keys


class TestPublishMonitorMessage(TestProducerBase):

    @property
    def monitor_window_in_sec(self):
        return get_config().monitoring_window_in_sec

    def publish_messages(self, messages, producer):
        for message in messages:
            producer.publish(message)
        producer.flush()
        producer.monitor.flush_buffered_info()

    def assert_equal_monitor_messages(
        self,
        actual_raw_messages,
        expected_topic,
        expected_messages_counts,
        expected_start_timestamp=0
    ):
        envelope = Envelope()

        expected_count_idx = 0
        for msg in actual_raw_messages:
            actual_message = self._get_actual_message(msg.message.value, envelope)
            assert actual_message.message_type == _ProtectedMessageType.monitor.name

            payload_data = actual_message.payload_data
            if payload_data['topic'] != expected_topic:
                continue

            self.assert_equal_monitor_message(
                actual_payload_data=payload_data,
                expected_topic=expected_topic,
                expected_message_count=expected_messages_counts[expected_count_idx],
                expected_start_timestamp=expected_start_timestamp
            )
            expected_start_timestamp += self.monitor_window_in_sec
            expected_count_idx += 1

    def _get_actual_message(self, raw_message, envelope):
        unpacked_message = envelope.unpack(raw_message)
        actual_message = Message(
            schema_id=unpacked_message['schema_id'],
            payload=unpacked_message['payload'],
            uuid=unpacked_message['uuid'],
            timestamp=unpacked_message['timestamp']
        )
        actual_message._message_type = unpacked_message['message_type']
        return actual_message

    def assert_equal_monitor_message(
        self,
        actual_payload_data,
        expected_topic,
        expected_message_count,
        expected_start_timestamp
    ):
        assert actual_payload_data['message_count'] == expected_message_count
        assert actual_payload_data['client_type'] == 'producer'
        assert actual_payload_data['start_timestamp'] == expected_start_timestamp
        assert actual_payload_data['topic'] == expected_topic

    def get_timestamp(self, monitor_start_time, timeslot):
        return monitor_start_time + self.monitor_window_in_sec * timeslot

    @pytest.fixture
    def create_message(self, payload, producer):
        def _create_message(schema, timeslot):
            monitor_start_time = producer.monitor.start_time
            return CreateMessage(
                schema.schema_id,
                payload=payload,
                timestamp=int(self.get_timestamp(monitor_start_time, timeslot))
            )
        return _create_message

    def test_monitoring_message_basic(
        self, producer, create_message, registered_schema
    ):
        message = create_message(registered_schema, timeslot=2.5)
        messages_to_publish = [message] * 10
        topic = message.topic

        with setup_capture_new_messages_consumer(
            topic
        ) as consumer, setup_capture_new_messages_consumer(
            producer.monitor.monitor_topic
        ) as monitor_consumer:
            self.publish_messages(messages_to_publish, producer)

            messages = consumer.get_messages(count=100)
            assert len(messages) == len(messages_to_publish)

            monitor_messages = monitor_consumer.get_messages(count=100)
            assert len(monitor_messages) == 3

            self.assert_equal_monitor_messages(
                actual_raw_messages=monitor_messages,
                expected_topic=topic,
                expected_messages_counts=[0, 0, 10],
                expected_start_timestamp=producer.monitor.start_time
            )

    def test_publish_messages_with_diff_timestamps(
        self, producer, create_message, registered_schema
    ):
        messages_to_publish = [
            create_message(registered_schema, timeslot=0.5),
            create_message(registered_schema, timeslot=1.5),
            create_message(registered_schema, timeslot=3.5)
        ]
        topic = messages_to_publish[0].topic

        with setup_capture_new_messages_consumer(
            topic
        ) as consumer, setup_capture_new_messages_consumer(
            producer.monitor.monitor_topic
        ) as monitor_consumer:
            self.publish_messages(messages_to_publish, producer)

            messages = consumer.get_messages(count=100)
            assert len(messages) == len(messages_to_publish)

            monitor_messages = monitor_consumer.get_messages(count=100)
            assert len(monitor_messages) == 4

            self.assert_equal_monitor_messages(
                actual_raw_messages=monitor_messages,
                expected_topic=topic,
                expected_messages_counts=[1, 1, 0, 1],
                expected_start_timestamp=producer.monitor.start_time
            )

    def test_publish_messages_with_diff_topic_and_timestamp(
        self,
        registered_schema,
        another_schema,
        topic,
        another_topic,
        producer,
        create_message
    ):
        messages_to_publish = [
            create_message(registered_schema, timeslot=0.5),
            create_message(another_schema, timeslot=0.8),
            create_message(registered_schema, timeslot=3.5),
            create_message(another_schema, timeslot=4),
            create_message(another_schema, timeslot=6),
        ]

        with setup_capture_new_messages_consumer(
            topic
        ) as consumer, setup_capture_new_messages_consumer(
            another_topic
        ) as another_consumer, setup_capture_new_messages_consumer(
            producer.monitor.monitor_topic
        ) as monitor_consumer:
            self.publish_messages(messages_to_publish, producer)

            assert len(consumer.get_messages(count=100)) == 2
            assert len(another_consumer.get_messages(count=100)) == 3

            monitor_messages = monitor_consumer.get_messages(count=100)
            assert len(monitor_messages) == 14

            self.assert_equal_monitor_messages(
                actual_raw_messages=monitor_messages,
                expected_topic=topic,
                expected_messages_counts=[1, 0, 0, 1, 0, 0, 0],
                expected_start_timestamp=producer.monitor.start_time
            )
            self.assert_equal_monitor_messages(
                actual_raw_messages=monitor_messages,
                expected_topic=another_topic,
                expected_messages_counts=[1, 0, 0, 0, 1, 0, 1],
                expected_start_timestamp=producer.monitor.start_time
            )

    def test_monitoring_system_dry_run(self, producer_name, team_name):
        producer = Producer(
            producer_name=producer_name,
            team_name=team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            dry_run=True
        )
        assert producer.monitor.dry_run is True


class TestEnsureMessagesPublished(TestProducerBase):
    number_of_messages = 5

    @pytest.fixture
    def random_schema(self, create_new_schema):
        return create_new_schema(source='ensure_published_source')

    @pytest.fixture
    def secondary_random_schema(self, create_new_schema):
        return create_new_schema(source='ensure_published_source_2')

    @pytest.fixture
    def topic(self, random_schema):
        return str(random_schema.topic.name)

    @pytest.fixture
    def secondary_topic(self, secondary_random_schema):
        return str(secondary_random_schema.topic.name)

    @pytest.fixture(params=[True, False])
    def topic_offsets(self, request, producer, random_schema, containers):
        is_fresh_topic = request.param
        if is_fresh_topic:
            containers.create_kafka_topic(str(random_schema.topic.name))
            return {}

        message = CreateMessage(random_schema.schema_id, payload=str('-1'))
        producer.publish(message)
        producer.flush()
        return producer.get_checkpoint_position_data().topic_to_kafka_offset_map

    @pytest.fixture
    def messages(self, random_schema):
        return [
            CreateMessage(
                random_schema.schema_id,
                payload=str(i),
                upstream_position_info={'position': i + 1}
            )
            for i in range(self.number_of_messages)
        ]

    @pytest.fixture
    def secondary_messages(self, secondary_random_schema):
        return [
            CreateMessage(
                secondary_random_schema.schema_id,
                payload=str(i),
                upstream_position_info={'position': i + 1}
            )
            for i in range(-2, 1)
        ]

    def test_ensure_messages_published_without_message(
        self, topic, producer, topic_offsets
    ):
        with setup_capture_new_messages_consumer(topic) as consumer:
            producer.ensure_messages_published([], topic_offsets)
            self._assert_all_messages_published(consumer, expected_payloads=[])

    def test_ensure_messages_published_when_unpublished(
        self, topic, messages, producer, topic_offsets
    ):
        self._test_success_ensure_messages_published(
            topic,
            messages,
            producer,
            topic_offsets,
            unpublished_count=len(messages)
        )

    def test_ensure_messages_published_when_partially_published(
        self, topic, messages, producer, topic_offsets
    ):
        self._test_success_ensure_messages_published(
            topic,
            messages,
            producer,
            topic_offsets,
            unpublished_count=2
        )

    def test_ensure_messages_published_when_all_published(
        self, topic, messages, producer, topic_offsets
    ):
        self._test_success_ensure_messages_published(
            topic,
            messages,
            producer,
            topic_offsets,
            unpublished_count=0
        )

    def _test_success_ensure_messages_published(
        self, topic, messages, producer, topic_offsets, unpublished_count
    ):
        messages_to_publish = len(messages) - unpublished_count
        messages_published_first = messages[:messages_to_publish]

        with setup_capture_new_messages_consumer(
            topic
        ) as consumer, mock.patch.object(
            data_pipeline.producer,
            'logger'
        ) as mock_logger:
            for message in messages_published_first:
                producer.publish(message)
            producer.flush()
            producer.position_data_callback = mock.Mock()

            producer.ensure_messages_published(messages, topic_offsets)

            if unpublished_count > 0:
                assert producer.position_data_callback.call_count == 1

            self._assert_all_messages_published(consumer)

            position_info = producer.get_checkpoint_position_data()
            last_position = position_info.last_published_message_position_info
            assert last_position['position'] == self.number_of_messages

            self._assert_logged_info_correct(
                mock_logger,
                messages_already_published=len(messages_published_first),
                topic=topic,
                topic_offsets=topic_offsets,
                message_count=len(messages)
            )

    def test_multitopic_offsets(
        self,
        topic,
        messages,
        secondary_topic,
        secondary_messages,
        producer,
        topic_offsets,
        containers
    ):
        """Publishes a single message on the secondary_topic, and all
        messages on the primary topic, simulating the case where publishes for
        one topic fail, while the other succeeds, and the one that succeeds
        comes later in time.  The goal is that the position data still reflects
        the original message ordering, irrespective of failure.
        """
        containers.create_kafka_topic(secondary_topic)
        with setup_capture_new_messages_consumer(
            secondary_topic
        ) as consumer:
            producer.publish(secondary_messages[0])
            for message in messages:
                producer.publish(message)
            producer.flush()

            producer.ensure_messages_published(
                secondary_messages + messages,
                topic_offsets
            )

            position_info = producer.get_checkpoint_position_data()
            last_position = position_info.last_published_message_position_info
            assert last_position['position'] == self.number_of_messages
            assert len(consumer.get_messages(10)) == len(secondary_messages)

    def test_ensure_messages_published_fails_when_overpublished(
        self, topic, messages, producer, topic_offsets
    ):
        for message in messages:
            producer.publish(message)
        producer.flush()

        with pytest.raises(
            PublicationUnensurableError
        ), mock.patch.object(
            data_pipeline.producer,
            'logger'
        ) as mock_logger:
            producer.ensure_messages_published(messages[:2], topic_offsets)

            self._assert_logged_info_correct(
                mock_logger,
                len(messages),
                topic,
                topic_offsets,
                message_count=len(messages[:2])
            )

    def test_forced_recovery_when_overpublished(
        self, topic, messages, producer, topic_offsets
    ):
        for message in messages:
            producer.publish(message)
        producer.flush()

        with reconfigure(
            force_recovery_from_publication_unensurable_error=True
        ), setup_capture_new_messages_consumer(
            topic
        ) as consumer, mock.patch.object(
            data_pipeline.producer,
            'logger'
        ) as mock_logger:
            producer.ensure_messages_published(messages[:2], topic_offsets)

            self._assert_logged_info_correct(
                mock_logger,
                len(messages),
                topic,
                topic_offsets,
                message_count=len(messages[:2])
            )

            assert len(consumer.get_messages(10)) == 2

    def test_ensure_messages_published_on_new_topic(
        self, create_new_schema, producer
    ):
        """When a topic doesn't exist, all of the messages on that topic should
        be published.
        """
        new_schema = create_new_schema(source='ensure_published_source_two')
        message = CreateMessage(new_schema.schema_id, payload=str('1'))
        topic = str(new_schema.topic.name)

        with attach_spy_on_func(producer, 'publish') as func_spy:
            producer.ensure_messages_published([message], {})
            assert func_spy.call_count == 1
        with setup_capture_new_messages_consumer(topic) as consumer:
            kafka_offset = 0
            consumer.seek(kafka_offset, 0)  # kafka_offset from head
            self._assert_all_messages_published(consumer, expected_payloads=[1])

    def _assert_logged_info_correct(
        self,
        mock_logger,
        messages_already_published,
        topic,
        topic_offsets,
        message_count
    ):
        assert mock_logger.info.call_count == 1
        (log_line,), _ = mock_logger.info.call_args
        logged_info = json.loads(log_line)

        assert logged_info['topic'] == topic
        assert logged_info['message_count'] == message_count
        assert logged_info['saved_offset'] == topic_offsets.get(topic, 0)
        assert logged_info['already_published_count'] == messages_already_published
        assert logged_info['high_watermark'] == (
            topic_offsets.get(topic, 0) + messages_already_published
        )

    def _assert_all_messages_published(self, consumer, expected_payloads=None):
        messages = consumer.get_messages(count=self.number_of_messages * 2)
        expected_payloads = (expected_payloads if expected_payloads is not None
                             else range(self.number_of_messages))
        envelope = Envelope()
        payloads = [
            int(envelope.unpack(message.message.value)['payload'])
            for message in messages
        ]
        assert payloads == expected_payloads


class TestPublishMessagesWithRetry(TestProducerBase):

    @pytest.fixture
    def another_message(self, another_schema, payload):
        return CreateMessage(another_schema.schema_id, payload=payload)

    @property
    def max_retry_count(self):
        return 3

    @property
    def retry_policy(self):
        return RetryPolicy(
            ExpBackoffPolicy(initial_delay_secs=0.1, max_delay_secs=0.5),
            max_retry_count=self.max_retry_count
        )

    @pytest.fixture(autouse=True)
    def patch_retry_policy(self, producer):
        producer._kafka_producer._publish_retry_policy = self.retry_policy

    @pytest.fixture(autouse=True)
    def set_topic_offsets_to_latest(self, producer, message, another_message):
        producer.publish(message)
        producer.publish(another_message)
        producer.flush()

    @pytest.yield_fixture(autouse=True)
    def setup_flush_time_limit(self):
        # publish all msgs together
        yield reconfigure(kafka_producer_flush_time_limit_seconds=10)

    def test_publish_succeeds_without_retry(self, topic, message, producer):
        with attach_spy_on_func(
            producer._kafka_producer.kafka_client,
            'send_produce_request'
        ) as send_request_spy, capture_new_messages(
            topic
        ) as get_messages:
            producer.publish(message)
            producer.flush()

            messages = get_messages()
            self.assert_equal_msgs(expected_msgs=[message], actual_msgs=messages)
            assert send_request_spy.call_count == 1

    def test_publish_fails_after_retry(self, message, producer):
        # TODO(DATAPIPE-606|clin) investigate better way than mocking response
        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=[FailedPayloadsError]
        ) as mock_send_request, capture_new_messages(
            message.topic
        ) as get_messages, pytest.raises(
            MaxRetryError
        ):
            producer.publish(message)
            producer.flush()

            messages = get_messages()
            assert len(messages) == 0
            assert mock_send_request.call_count == self.max_retry_count

    def test_publish_to_new_topic(self, create_new_schema, producer):
        new_schema = create_new_schema(source='retry_source')
        message = CreateMessage(new_schema.schema_id, payload=str('1'))

        with attach_spy_on_func(
            producer._kafka_producer.kafka_client,
            'send_produce_request'
        ) as send_request_spy:
            send_request_spy.reset()
            producer.publish(message)
            producer.flush()
            # it should fail at least the 1st time because the topic doesn't
            # exist. Depending on how fast the topic is created, it could retry
            # more than 2 times.
            assert send_request_spy.call_count >= 2

        messages = self.get_messages_from_start(message.topic)
        self.assert_equal_msgs(expected_msgs=[message], actual_msgs=messages)

    def test_publish_one_msg_succeeds_one_fails_after_retry(
        self,
        message,
        another_message,
        topic,
        producer
    ):
        # TODO(DATAPIPE-606|clin) investigate better way than mocking response
        mock_response = ProduceResponse(topic, partition=0, error=0, offset=1)
        fail_response = FailedPayloadsError(payload=mock.Mock())
        side_effect = ([[mock_response, fail_response]] +
                       [[fail_response]] * self.max_retry_count)
        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=side_effect
        ), pytest.raises(
            MaxRetryError
        ) as e:
            producer.publish(message)
            producer.publish(another_message)
            producer.flush()

            self.assert_last_retry_result(
                e.value.last_result,
                another_message,
                expected_published_msgs_count=1
            )

    def test_retry_false_failed_publish(self, message, producer):
        # TODO(DATAPIPE-606|clin) investigate better way than mocking response
        orig_func = producer._kafka_producer.kafka_client.send_produce_request

        def run_original_func_but_throw_exception(*args, **kwargs):
            orig_func(*args, **kwargs)
            raise RandomException()

        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=run_original_func_but_throw_exception
        ) as mock_send_request, capture_new_messages(
            message.topic
        ) as get_messages:
            mock_send_request.reset()
            producer.publish(message)
            producer.flush()

            messages = get_messages()
            self.assert_equal_msgs(expected_msgs=[message], actual_msgs=messages)
            assert mock_send_request.call_count == 1  # should be no retry

    def test_retry_failed_publish_without_highwatermark(self, message, producer):
        # TODO(DATAPIPE-606|clin) investigate better way than mocking response
        with mock.patch.object(
            producer._kafka_producer.kafka_client,
            'send_produce_request',
            side_effect=[FailedPayloadsError]
        ) as mock_send_request, mock.patch(
            'data_pipeline._kafka_util.get_topics_watermarks',
            side_effect=Exception
        ), capture_new_messages(
            message.topic
        ) as get_messages, pytest.raises(
            MaxRetryError
        ) as e:
            producer.publish(message)
            producer.flush()

            assert mock_send_request.call_count == 1  # should be no retry
            self.assert_last_retry_result(
                e.value.last_result,
                message,
                expected_published_msgs_count=0
            )

            messages = get_messages()
            assert len(messages) == 0

    def get_messages_from_start(self, topic_name):
        with setup_capture_new_messages_consumer(topic_name) as consumer:
            consumer.seek(0, 0)  # set to the first message
            return consumer.get_messages()

    def assert_equal_msgs(self, expected_msgs, actual_msgs):
        envelope = Envelope()
        assert len(actual_msgs) == len(expected_msgs)
        for actual, expected in zip(actual_msgs, expected_msgs):
            actual_payload = envelope.unpack(actual.message.value)['payload']
            expected_payload = expected.payload
            assert actual_payload == expected_payload

    def assert_last_retry_result(
        self, last_retry_result, message, expected_published_msgs_count
    ):
        expected_requests = [ProduceRequest(
            topic=message.topic,
            partition=0,
            messages=[_prepare(_EnvelopeAndMessage(Envelope(), message))]
        )]
        assert last_retry_result.unpublished_requests == expected_requests
        assert last_retry_result.total_published_message_count == expected_published_msgs_count
