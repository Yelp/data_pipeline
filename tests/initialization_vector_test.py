# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline._encryption_helper import _EncryptionAlgorithmAVSCStore
from data_pipeline._encryption_helper import initialization_vector_info
from data_pipeline.initialization_vector import InitializationVector


class TestInitializationVector(object):

    @pytest.fixture
    def vector_schema_id(self):
        return _EncryptionAlgorithmAVSCStore().get_schema_id(initialization_vector_info)

    @pytest.fixture
    def vector_payload_data(self):
        return b'0000000000000000'

    @pytest.fixture
    def new_initialization_vector(
        self,
        vector_schema_id,
        vector_payload_data,
        containers
    ):
        return InitializationVector(vector_schema_id, vector_payload_data)

    @pytest.fixture(params=[
        {'schema_id': 10, 'initialization_vector_array': bytes(10)}
    ])
    def invalid_arg_value(self, request):
        return request.param

    def test_create_vector_fails_with_bad_arg_values(self, invalid_arg_value, containers):
        with pytest.raises(TypeError):
            InitializationVector(**invalid_arg_value)

    def test_initialization_vector_creation(self, new_initialization_vector):
        assert isinstance(new_initialization_vector, InitializationVector)
        assert isinstance(new_initialization_vector.avro_repr['payload'], bytes)
