# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.envelope import _AvroStringReader
from data_pipeline.envelope import _AvroStringWriter
from data_pipeline.envelope import _get_avro_schema_object
from data_pipeline.sample_data_loader import SampleDataLoader


class TestSampleDataLoader(object):

    @pytest.fixture
    def loader(self):
        return SampleDataLoader()

    def test_get_raw_business_sample_data_and_schema(self, loader):
        data, schema = loader.get_raw_business_sample_data_and_schema()
        assert len(data) > 0
        avro_schema = _get_avro_schema_object(schema)
        writer = _AvroStringWriter(schema=avro_schema)
        reader = _AvroStringReader(reader_schema=avro_schema, writer_schema=avro_schema)
        encoded_payload = writer.encode(data[0])
        decoded_payload = reader.decode(encoded_payload)
        assert data[0] == decoded_payload

    def test_get_same_item_always_is_equal(self, loader):
        env1 = loader.get_data('envelope.avsc')
        assert env1 == loader.get_data('envelope.avsc')

    def test_non_existant_file_raises(self, loader):
        with pytest.raises(IOError):
            loader.get_data('does_not_exist.txt')
