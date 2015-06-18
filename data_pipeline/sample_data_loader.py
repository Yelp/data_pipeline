# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os.path
import pickle
import sys

from data_pipeline._avro_util import AvroStringReader
from data_pipeline._avro_util import get_avro_schema_object


class SampleDataLoader(object):

    """
    A utility class for testing with the provided sample data. Performs some
    basic caching and helper methods to make testing go smoother. The goal is
    to keep all avro usage outside of 'mainline' package code
    """

    _sample_data = {}
    _schemas = {}
    base_path = os.path.join(
        os.path.abspath(os.path.dirname(sys.modules[__name__].__file__)),
        'sample_data'
    )

    def get_data(self, file_name):
        """
        :param str file_name: The file in the sample_data directory to access
        :rtype: str
        """
        path = self._get_path(file_name)
        if path not in self._sample_data:
            self._sample_data[path] = open(path).read()
        return self._sample_data[path]

    def _get_path(self, file_name):
        return os.path.join(self.base_path, file_name)

    def get_raw_business_sample_data_and_schema(self):
        """ Load the sample envelope, business schema, and pickled business
        messages (from the data pipeline tracer-bullet) and prepare them for
        test usage.

        :return: The list of extracted message data and the avro schema
        :rtype: (list[dict], str)
        """
        envelope_schema = get_avro_schema_object(
            self.get_data('envelope.avsc')
        )
        business_schema = get_avro_schema_object(
            self.get_data('raw_business.avsc')
        )
        business_reader = AvroStringReader(
            reader_schema=business_schema,
            writer_schema=business_schema
        )
        envelope_reader = AvroStringReader(
            reader_schema=envelope_schema,
            writer_schema=envelope_schema
        )
        kafka_items = pickle.loads(self.get_data('raw_messages.p'))
        sample_data = []
        for item in kafka_items:
            envelope_data = envelope_reader.decode(item.message.value)
            data = business_reader.decode(envelope_data['payload'])
            sample_data.append(data)
        return sample_data, business_schema
