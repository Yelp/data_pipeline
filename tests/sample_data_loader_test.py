# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.avro_util import decode_payload
from data_pipeline.avro_util import encode_payload
from data_pipeline.sample_data_loader import SampleDataLoader


class TestSampleDataLoader(object):

    @pytest.fixture
    def loader(self):
        return SampleDataLoader()

    def test_get_raw_business_sample_data_and_schema(self, loader):
        data, schema = loader.get_raw_business_sample_data_and_schema()
        assert len(data) > 0
        encoded_payload = encode_payload(data[0], schema)
        decoded_payload = decode_payload(encoded_payload, schema)
        assert data[0] == decoded_payload

    def test_get_same_item_always_is_equal(self, loader):
        env1 = loader.get_data('envelope.avsc')
        assert env1 == loader.get_data('envelope.avsc')

    def test_non_existant_file_raises(self, loader):
        with pytest.raises(IOError):
            loader.get_data('does_not_exist.txt')
