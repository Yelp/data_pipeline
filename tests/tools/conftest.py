# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import pytest

from data_pipeline.tools.schema_ref import SchemaRef


logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture
def good_source_ref():
    return {
        "category": "test_category",
        "file_display": "path/to/test.py",
        "fields": [
            {
                "note": "Notes for good_field",
                "doc": "Docs for good_field",
                "name": "good_field"
            },
            {
                "name": "bad_field"
            },
        ],
        "owner_email": "test@yelp.com",
        "namespace": "test_namespace",
        "file_url": "http://www.test.com/",
        "note": "Notes for good_source",
        "source": "good_source",
        "doc": "Docs for good_source",
        "contains_pii": False
    }


@pytest.fixture
def bad_source_ref():
    return {"fields": [], "source": "bad_source"}


@pytest.fixture
def schema_ref_dict(good_source_ref, bad_source_ref):
    return {
        "doc_source": "http://www.docs-r-us.com/good",
        "docs": [
            good_source_ref,
            bad_source_ref
        ],
        "doc_owner": "test@yelp.com"
    }


@pytest.fixture
def schema_ref_defaults():
    return {
        'doc_owner': 'test_doc_owner@yelp.com',
        'owner_email': 'test_owner@yelp.com',
        'namespace': 'test_namespace',
        'doc': 'test_doc',
        'contains_pii': False,
        'category': 'test_category'
    }


@pytest.fixture
def schema_ref(schema_ref_dict, schema_ref_defaults):
    return SchemaRef(
        schema_ref=schema_ref_dict,
        defaults=schema_ref_defaults
    )
