# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import warnings

import pytest
from kafka import create_message
from kafka.common import OffsetAndMessage

from data_pipeline import message as dp_message
from data_pipeline._fast_uuid import FastUUID
from data_pipeline.envelope import Envelope
from data_pipeline.message import create_from_offset_and_message
from data_pipeline.message import CreateMessage
from data_pipeline.message import InvalidOperation
from data_pipeline.message import MetaAttribute
from data_pipeline.message import NoEntryPayload
from data_pipeline.message import PayloadFieldDiff
from data_pipeline.message_type import _ProtectedMessageType
from data_pipeline.message_type import MessageType
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from tests.helpers.config import reconfigure
from tests.helpers.mock_utils import attach_spy_on_func


@pytest.mark.usefixtures("containers")
class SharedMessageTest(object):

    @pytest.fixture
    def message(self, valid_message_data):
        return self.message_class(**valid_message_data)

    @pytest.fixture(params=[
        None,
        100,
        ['test'],
        {'data': 'foo'}
    ])
    def invalid_payload(self, request):
        return request.param

    @pytest.fixture(params=[None])
    def invalid_payload_data(self, request):
        return request.param

    @pytest.fixture
    def pii_message(self, valid_message_data, pii_schema):
        message_data = self._make_message_data(
            valid_message_data,
            schema_id=pii_schema.schema_id
        )
        return self.message_class(**message_data)

    def test_warning_from_explicit_topic(self, valid_message_data):
        data_with_topic = self._make_message_data(
            valid_message_data,
            topic=str('explicit_topic')
        )
        with warnings.catch_warnings(record=True) as w:
            self.message_class(**data_with_topic)
            assert len(w) == 1
            assert "Passing in topics explicitly is deprecated." in w[0].message

    def test_get_topic_from_schematizer_by_default(
        self,
        registered_schema,
        message
    ):
        assert message.topic == str(registered_schema.topic.name)

    def test_rejects_non_numeric_schema_id(self, valid_message_data):
        self._assert_invalid_data(valid_message_data, schema_id='123')

    def test_rejects_junk_uuid(self, valid_message_data):
        self._assert_invalid_data(valid_message_data, uuid='junk')

    def test_rejects_non_dicts_in_upstream_position_info(self, valid_message_data):
        self._assert_invalid_data(valid_message_data, upstream_position_info='test')
        self._assert_invalid_data(valid_message_data, upstream_position_info=['test'])

    def test_rejects_non_kafka_position_info(self, valid_message_data):
        self._assert_invalid_data(valid_message_data, kafka_position_info=123)

    def test_rejects_invalid_payload(self, valid_message_data, invalid_payload):
        self._assert_invalid_data(
            valid_message_data,
            payload=invalid_payload,
            payload_data=None
        )

    def test_rejects_both_payload_and_payload_data(self, valid_message_data):
        self._assert_invalid_data(
            valid_message_data,
            payload=bytes(10),
            payload_data={'data': 'foo'}
        )

    def test_rejects_invalid_payload_data(
        self,
        valid_message_data,
        invalid_payload_data
    ):
        self._assert_invalid_data(
            valid_message_data,
            payload=None,
            payload_data=invalid_payload_data
        )

    def test_reject_encrypted_message_without_encryption(self, pii_message):
        with pytest.raises(ValueError):
            pii_message.encryption_type

    def _assert_invalid_data(self, valid_data, error=TypeError, **data_overrides):
        invalid_data = self._make_message_data(valid_data, **data_overrides)
        with pytest.raises(error):
            self.message_class(**invalid_data)

    def _make_message_data(self, valid_data, **overrides):
        message_data = dict(valid_data)
        message_data.update(**overrides)
        return message_data

    def test_generates_uuid(self, message):
        assert isinstance(message.uuid, bytes) and len(message.uuid) == 16

    def test_accepts_dicts_in_upstream_position_info(self, valid_message_data):
        message_data = self._make_message_data(
            valid_message_data,
            upstream_position_info=dict(something='some_unicode')
        )
        message = self.message_class(**message_data)
        assert isinstance(message, self.message_class)

    def test_message_type(self, message):
        assert message.message_type == self.expected_message_type

    def test_get_contains_pii_from_schematizer(self, message, registered_schema):
        assert message.contains_pii == registered_schema.topic.contains_pii

    def test_specify_contains_pii_triggers_warnings(self, valid_message_data):
        message_data = self._make_message_data(
            valid_message_data,
            contains_pii=False
        )
        with warnings.catch_warnings(record=True) as warns:
            self.message_class(**message_data)
            assert len(warns) == 1
            contains_pii_warning = warns[0]
            assert issubclass(contains_pii_warning.category, DeprecationWarning)
            assert ('contains_pii is deprecated. Please stop passing it in.'
                    in contains_pii_warning.message)

    def test_specify_keys_triggers_warnings(self, valid_message_data):
        message_data = self._make_message_data(
            valid_message_data,
            keys=(1, 2)
        )
        with warnings.catch_warnings(record=True) as warns:
            self.message_class(**message_data)
            assert len(warns) == 1
            keys_warning = warns[0]
            assert issubclass(keys_warning.category, DeprecationWarning)
            assert ('Passing in keys explicitly is deprecated.'
                    in keys_warning.message)

    @pytest.mark.parametrize('invalid_meta', ['not list', ['not_MetaAttribute']])
    def test_rejects_invalid_meta_type(self, valid_message_data, invalid_meta):
        self._assert_invalid_data(valid_message_data, meta=invalid_meta)

    @pytest.fixture
    def meta_attr_payload_data(self):
        return {'good_payload': 26}

    @pytest.fixture
    def valid_meta_param(self, meta_attr_payload_data, registered_meta_attribute):
        meta_attr = MetaAttribute(
            schema_id=registered_meta_attribute.schema_id,
            payload_data=meta_attr_payload_data
        )
        return [meta_attr]

    def _get_dry_run_message_with_meta(self, valid_message_data, meta_param=None):
        message_data = self._make_message_data(
            valid_message_data,
            meta=meta_param
        )
        return self.message_class(**message_data)

    def test_accepts_no_meta(self, valid_message_data):
        dry_run_message = self._get_dry_run_message_with_meta(valid_message_data)
        assert dry_run_message.meta is None

    def test_accepts_valid_meta(
        self,
        valid_message_data,
        valid_meta_param,
        meta_attr_payload_data
    ):
        dry_run_message = self._get_dry_run_message_with_meta(
            valid_message_data,
            valid_meta_param
        )
        assert dry_run_message.meta[0].schema_id == valid_meta_param[0].schema_id
        assert dry_run_message.meta[0].payload_data == meta_attr_payload_data

    def test_dry_run(self, valid_message_data):
        payload_data = {'data': 'test'}
        message_data = self._make_message_data(
            valid_message_data,
            payload=None,
            payload_data=payload_data,
            dry_run=True
        )
        dry_run_message = self.message_class(**message_data)
        assert dry_run_message.payload == repr(payload_data)

    def test_equality(self, valid_message_data):
        message1 = self.message_class(**valid_message_data)
        message2 = self.message_class(**valid_message_data)
        assert message1 == message1
        assert message1 == message2
        assert message2 == message2

    def test_inequality(self, valid_message_data):
        message1 = self.message_class(**valid_message_data)

        message_data2 = self._make_message_data(
            valid_message_data,
            dry_run=not message1.dry_run
        )
        message2 = self.message_class(**message_data2)

        assert message1 != message2

    def test_message_str(self, message):
        actual = str(message)
        expected = {
            'message_type': self.expected_message_type.name,
            'schema_id': message.schema_id,
            'timestamp': message.timestamp,
            'meta': message._get_meta_attr_avro_repr(),
            'encryption_type': message.encryption_type,
            'uuid': message.uuid_hex,
            'payload_data': message.payload_data
        }
        # only use eval to get the original dict when the string is trusted
        assert eval(actual) == expected

    def test_message_str_with_pii(self, pii_message):
        with reconfigure(encryption_type='AES_MODE_CBC-1'):
            actual = str(pii_message)
            expected_payload_data = {u'good_field': u"<type 'int'>"}
            expected = {
                'message_type': self.expected_message_type.name,
                'schema_id': pii_message.schema_id,
                'timestamp': pii_message.timestamp,
                'meta': [pii_message.meta[0]._asdict()],
                'encryption_type': pii_message.encryption_type,
                'uuid': pii_message.uuid_hex,
                'payload_data': expected_payload_data,
            }
            # only use eval to get the original dict when the string is trusted
            assert eval(actual) == expected

    def assert_equal_decrypted_payload(
        self,
        message,
        actual_encrypted_payload,
        expected_decrypted_payload
    ):
        assert message._encryption_helper.decrypt_payload(
            actual_encrypted_payload
        ) == expected_decrypted_payload

    def test_setup_contains_pii_from_schematizer_once(self, message):
        schematizer_client = get_schematizer()
        with attach_spy_on_func(schematizer_client, 'get_schema_by_id') as spy:
            message.contains_pii
            assert spy.call_count == 1
        with attach_spy_on_func(schematizer_client, 'get_schema_by_id') as spy:
            message.contains_pii
            assert spy.call_count == 0

    def test_setup_encryption_type_from_config_once(self, pii_message):
        with reconfigure(encryption_type='Algorithm_one-1'):
            assert pii_message.encryption_type == 'Algorithm_one-1'
        with reconfigure(encryption_type='Algorithm_two-1'):
            assert pii_message.encryption_type == 'Algorithm_one-1'


class PayloadOnlyMessageTest(SharedMessageTest):

    @pytest.fixture(params=[(bytes(10), None), (None, {'good_field': 1})])
    def valid_message_data(self, request, registered_schema):
        payload, payload_data = request.param
        return {
            'schema_id': registered_schema.schema_id,
            'payload': payload,
            'payload_data': payload_data,
            'uuid': FastUUID().uuid4()
        }

    def test_rejects_previous_payload(self, message):
        with pytest.raises(AttributeError):
            message.previous_payload

    def test_rejects_previous_payload_data(self, message):
        with pytest.raises(AttributeError):
            message.previous_payload_data

    def test_encrypted_message(self, pii_schema, payload, example_payload_data):
        with reconfigure(encryption_type='AES_MODE_CBC-1'):
            test_params = [(payload, None), (None, example_payload_data)]
            for _payload, _payload_data in test_params:
                message = self.message_class(
                    schema_id=pii_schema.schema_id,
                    payload=_payload,
                    payload_data=_payload_data,
                )
                assert message.payload == payload
                assert message.payload_data == example_payload_data
                self.assert_equal_decrypted_payload(
                    message,
                    actual_encrypted_payload=message.avro_repr['payload'],
                    expected_decrypted_payload=payload
                )

    def test_keys(
        self,
        registered_schema_with_pkey,
        example_payload_data_with_pkeys,
        example_payload_with_pkeys
    ):
        test_params = [
            (example_payload_with_pkeys, None),
            (None, example_payload_data_with_pkeys)
        ]
        expected_keys = {
            "field2": example_payload_data_with_pkeys["field2"],
            "field1": example_payload_data_with_pkeys["field1"],
            "field3": example_payload_data_with_pkeys["field3"],
        }

        for _payload, _payload_data in test_params:
            message = self.message_class(
                schema_id=registered_schema_with_pkey.schema_id,
                payload=_payload,
                payload_data=_payload_data,
            )
            assert message.keys == expected_keys


class TestCreateMessage(PayloadOnlyMessageTest):

    @property
    def message_class(self):
        return dp_message.CreateMessage

    @property
    def expected_message_type(self):
        return MessageType.create

    def test_payload_diff(self, valid_message_data):
        valid_message_data.pop('payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            payload_data={'key1': 1, 'key2': 20}
        )
        message = self.message_class(**message_data)

        expected = {
            'key1': PayloadFieldDiff(
                old_value=NoEntryPayload,
                current_value=1
            ),
            'key2': PayloadFieldDiff(
                old_value=NoEntryPayload,
                current_value=20
            )
        }
        assert message.payload_diff == expected


class TestLogMessage(PayloadOnlyMessageTest):
    @property
    def message_class(self):
        return dp_message.LogMessage

    @property
    def expected_message_type(self):
        return MessageType.log

    def test_payload_diff_raises_exception(self, valid_message_data):
        valid_message_data.pop('payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            payload_data={'key1': 1, 'key2': 20}
        )
        message = self.message_class(**message_data)
        with pytest.raises(InvalidOperation):
            message.payload_diff


class TestMonitorMessage(PayloadOnlyMessageTest):
    @property
    def message_class(self):
        return dp_message.MonitorMessage

    @property
    def expected_message_type(self):
        return _ProtectedMessageType.monitor

    def test_payload_diff_raises_exception(self, valid_message_data):
        valid_message_data.pop('payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            payload_data={'key1': 1, 'key2': 20}
        )
        message = self.message_class(**message_data)
        with pytest.raises(InvalidOperation):
            message.payload_diff


class TestRefreshMessage(PayloadOnlyMessageTest):

    @property
    def message_class(self):
        return dp_message.RefreshMessage

    @property
    def expected_message_type(self):
        return MessageType.refresh

    def test_payload_diff_raises_exception(self, valid_message_data):
        valid_message_data.pop('payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            payload_data={'key1': 1, 'key2': 20}
        )
        message = self.message_class(**message_data)
        with pytest.raises(InvalidOperation):
            message.payload_diff


class TestDeleteMessage(PayloadOnlyMessageTest):

    @property
    def message_class(self):
        return dp_message.DeleteMessage

    @property
    def expected_message_type(self):
        return MessageType.delete

    def test_payload_diff(self, valid_message_data):
        valid_message_data.pop('payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            payload_data={'key1': 1, 'key2': 20}
        )
        message = self.message_class(**message_data)

        expected = {
            'key1': PayloadFieldDiff(
                old_value=1,
                current_value=NoEntryPayload
            ),
            'key2': PayloadFieldDiff(
                old_value=20,
                current_value=NoEntryPayload
            )
        }
        assert message.payload_diff == expected


class TestUpdateMessage(SharedMessageTest):

    @property
    def message_class(self):
        return dp_message.UpdateMessage

    @property
    def expected_message_type(self):
        return MessageType.update

    @pytest.fixture(params=[
        (bytes(10), None, bytes(100), None),
        (None, {'good_field': 1}, None, {'good_field': 2})
    ])
    def valid_message_data(self, request, registered_schema):
        payload, payload_data, previous_payload, previous_payload_data = request.param
        return {
            'schema_id': registered_schema.schema_id,
            'payload': payload,
            'payload_data': payload_data,
            'previous_payload': previous_payload,
            'previous_payload_data': previous_payload_data,
            'uuid': FastUUID().uuid4()
        }

    def test_rejects_invalid_previous_payload(
        self,
        valid_message_data,
        invalid_payload
    ):
        self._assert_invalid_data(
            valid_message_data,
            previous_payload=invalid_payload,
            previous_payload_data=None
        )

    def test_rejects_both_previous_payload_and_payload_data(
        self,
        valid_message_data
    ):
        self._assert_invalid_data(
            valid_message_data,
            previous_payload=bytes(10),
            previous_payload_data={'foo': 'bar'}
        )

    def test_rejects_invalid_previous_payload_data(
        self,
        valid_message_data,
        invalid_payload_data
    ):
        self._assert_invalid_data(
            valid_message_data,
            previous_payload=None,
            previous_payload_data=invalid_payload_data
        )

    def test_encrypted_message(self, pii_schema, payload, example_payload_data):
        # TODO [clin|DATAPIPE-851] let's see if this can be refactored
        with reconfigure(encryption_type='AES_MODE_CBC-1'):
            test_params = [(payload, None), (None, example_payload_data)]
            for _payload, _payload_data in test_params:
                message = self.message_class(
                    schema_id=pii_schema.schema_id,
                    payload=_payload,
                    previous_payload=_payload,
                    payload_data=_payload_data,
                    previous_payload_data=_payload_data
                )
                assert message.payload == payload
                assert message.previous_payload == payload
                assert message.payload_data == example_payload_data
                assert message.previous_payload_data == example_payload_data
                self.assert_equal_decrypted_payload(
                    message,
                    actual_encrypted_payload=message.avro_repr['payload'],
                    expected_decrypted_payload=payload
                )
                self.assert_equal_decrypted_payload(
                    message,
                    actual_encrypted_payload=message.avro_repr['previous_payload'],
                    expected_decrypted_payload=payload
                )

    def test_payload_diff(self, valid_message_data):
        valid_message_data.pop('payload', None)
        valid_message_data.pop('previous_payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            previous_payload_data={'key1': 1, 'key2': 2},
            payload_data={'key1': 1, 'key2': 20}
        )
        message = self.message_class(**message_data)

        expected = {'key2': PayloadFieldDiff(old_value=2, current_value=20)}
        assert message.payload_diff == expected
        assert message.has_changed

    def test_no_payload_diff(self, valid_message_data):
        valid_message_data.pop('payload', None)
        valid_message_data.pop('previous_payload', None)
        message_data = self._make_message_data(
            valid_message_data,
            previous_payload_data={'key1': 1, 'key2': 2},
            payload_data={'key1': 1, 'key2': 2}
        )
        message = self.message_class(**message_data)

        assert message.payload_diff == {}
        assert not message.has_changed

    def test_message_str(self, message):
        actual = str(message)
        expected = {
            'message_type': self.expected_message_type.name,
            'schema_id': message.schema_id,
            'timestamp': message.timestamp,
            'meta': message._get_meta_attr_avro_repr(),
            'encryption_type': message.encryption_type,
            'uuid': message.uuid_hex,
            'payload_data': message.payload_data,
            'previous_payload_data': message.previous_payload_data
        }
        # only use eval to get the original dict when the string is trusted
        assert eval(actual) == expected

    def test_message_str_with_pii(self, pii_message):
        with reconfigure(encryption_type='AES_MODE_CBC-1'):
            actual = str(pii_message)
            expected_payload_data = {u'good_field': u"<type 'int'>"}
            expected_previous_payload_data = {u'good_field': u"<type 'int'>"}
            expected = {
                'message_type': self.expected_message_type.name,
                'schema_id': pii_message.schema_id,
                'timestamp': pii_message.timestamp,
                'meta': [pii_message.meta[0]._asdict()],
                'encryption_type': pii_message.encryption_type,
                'uuid': pii_message.uuid_hex,
                'payload_data': expected_payload_data,
                'previous_payload_data': expected_previous_payload_data
            }
            # only use eval to get the original dict when the string is trusted
            assert eval(actual) == expected


class TestCreateFromMessageAndOffset(object):

    @pytest.fixture
    def offset_and_message(self, message):
        return OffsetAndMessage(0, create_message(Envelope().pack(message)))

    def test_create_from_offset_and_message(self, offset_and_message, message):
        extracted_message = create_from_offset_and_message(
            offset_and_message=offset_and_message
        )
        assert extracted_message.message_type == message.message_type
        assert extracted_message.payload == message.payload
        assert extracted_message.payload_data == message.payload_data
        assert extracted_message.schema_id == message.schema_id
        assert extracted_message.timestamp == message.timestamp
        assert extracted_message.topic == message.topic
        assert extracted_message.uuid == message.uuid

    def test_create_from_offset_and_message_with_reader_schema_specified(
        self,
        registered_schema,
        registered_compatible_schema,
        compatible_payload_data,
        example_payload_data,
    ):
        unpacked_message = CreateMessage(
            schema_id=registered_schema.schema_id,
            payload_data=example_payload_data,
            timestamp=1500,
        )
        offset_and_message = OffsetAndMessage(
            0,
            create_message(Envelope().pack(unpacked_message))
        )
        extracted_message = create_from_offset_and_message(
            offset_and_message=offset_and_message,
            reader_schema_id=registered_compatible_schema.schema_id
        )
        assert extracted_message.schema_id == registered_schema.schema_id
        assert extracted_message.topic == registered_schema.topic.name
        assert extracted_message.reader_schema_id == registered_compatible_schema.schema_id
        assert extracted_message.payload_data == compatible_payload_data

    def test_create_from_offset_and_message_with_no_reader_schema_specified(
        self,
        registered_schema,
        payload,
        example_payload_data
    ):
        unpacked_message = CreateMessage(
            schema_id=registered_schema.schema_id,
            payload=payload,
            timestamp=1500,
        )
        offset_and_message = OffsetAndMessage(
            0,
            create_message(Envelope().pack(unpacked_message))
        )

        extracted_message = create_from_offset_and_message(
            offset_and_message=offset_and_message,
            reader_schema_id=None
        )
        assert extracted_message.schema_id == registered_schema.schema_id
        assert extracted_message.topic == registered_schema.topic.name
        assert extracted_message.reader_schema_id == registered_schema.schema_id
        assert extracted_message.payload_data == example_payload_data
