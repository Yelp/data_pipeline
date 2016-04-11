# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import simplejson

from data_pipeline.config import get_config
from data_pipeline.tools.introspector.base import IntrospectorBatch

@pytest.mark.usefixtures('containers')
class TestBaseCommand(object):

    @pytest.fixture
    def batch(self, containers):
        batch = IntrospectorBatch("data_pipeline_introspector_base")
        batch.log.debug = mock.Mock()
        batch.log.info = mock.Mock()
        batch.log.warning = mock.Mock()
        return batch

    @property
    def source_owner_email(self):
        return "bam+test+introspection@yelp.com"

    def _get_client(self):
        return get_config().schematizer_client

    def _register_avro_schema(self,  namespace, source, two_fields, **overrides):
        fields = [{'type': 'int', 'name': 'foo'}]
        if two_fields:
            fields.append({'type': 'int', 'name': 'bar'})
        schema_json = {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'fields': fields
        }
        params = {
            'namespace': namespace,
            'source': source,
            'source_owner_email': self.source_owner_email,
            'schema': simplejson.dumps(schema_json),
            'contains_pii': False
        }
        if overrides:
            params.update(**overrides)
        return self._get_client().schemas.register_schema(body=params).result()

    @pytest.fixture
    def namespace_one(self):
        return "namespace_one"

    @pytest.fixture
    def namespace_two(self):
        return "namespace_two"

    @pytest.fixture
    def source_one_active(self):
        return "source_one_active"

    @pytest.fixture
    def source_one_inactive(self):
        return "source_one_inactive"

    @pytest.fixture
    def source_two_active(self):
        return "source_two_active"

    @pytest.fixture
    def source_two_inactive(self):
        return "source_two_inactive"

    @pytest.fixture
    def schema_one_active(self, containers, namespace_one, source_one_active):
        return self._register_avro_schema(namespace_one, source_one_active, two_fields=False)

    @pytest.fixture
    def schema_one_inactive(self, containers, namespace_one, source_one_inactive):
        return self._register_avro_schema(namespace_one, source_one_inactive, two_fields=True)

    @pytest.fixture
    def schema_two_active(self, containers, namespace_two, source_two_active):
        return self._register_avro_schema(namespace_two, source_two_active, two_fields=False)

    @pytest.fixture
    def schema_two_inactive(self, containers, namespace_two, source_two_inactive):
        return self._register_avro_schema(namespace_two, source_two_inactive, two_fields=True)

    @pytest.fixture(autouse=True)
    def topic_one_inactive(self, containers, schema_one_inactive):
        topic = schema_one_inactive.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True)
    def topic_one_active(self, containers, schema_one_active):
        topic = schema_one_active.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True)
    def topic_two_inactive(self, containers, schema_two_inactive):
        topic = schema_two_inactive.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    @pytest.fixture(autouse=True)
    def topic_two_active(self, containers, schema_two_active):
        topic = schema_two_active.topic
        containers.create_kafka_topic(str(topic.name))
        return topic

    def test_topic_creation(self, batch, containers, topic_one_inactive, topic_two_active):
        x = topic_one_inactive
        y = topic_two_active
        pass
