# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.base_meta_attribute import BaseMetaAttribute


class TestBaseMetaAttribute(object):

    @pytest.fixture(params=[
        'source',
        'namespace',
        'owner_email',
        'payload'
    ])
    def property_to_implement(self, request):
        return request.param

    def test_property_not_implemented(self, property_to_implement):
        with pytest.raises(NotImplementedError):
            BaseMetaAttribute().__getattribute__(property_to_implement)
