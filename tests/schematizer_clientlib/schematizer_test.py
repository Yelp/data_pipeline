# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random
import time
from datetime import datetime

import mock
import pytest
import simplejson
from requests import ConnectionError
from swaggerpy import exception as swaggerpy_exc

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.models.data_source_type_enum import \
    DataSourceTypeEnum
from data_pipeline.schematizer_clientlib.models.namespace import Namespace
from data_pipeline.schematizer_clientlib.models.target_schema_type_enum import \
    TargetSchemaTypeEnum
from data_pipeline.schematizer_clientlib.schematizer import SchematizerClient


@pytest.mark.usefixtures('containers')
class SchematizerClientTestBase(object):

    @pytest.fixture
    def schematizer(self, containers):
        return SchematizerClient()

    def attach_spy_on_api(self, resource, api_name):
        original_func = getattr(resource, api_name)

        def attach_spy(*args, **kwargs):
            return original_func(*args, **kwargs)

        return mock.patch.object(resource, api_name, side_effect=attach_spy)

    @pytest.fixture(scope='class')
    def yelp_namespace(self):
        return 'yelp_{0}'.format(random.random())

    @pytest.fixture(scope='class')
    def aux_namespace(self):
        return 'aux_{0}'.format(random.random())

    @pytest.fixture(scope='class')
    def biz_src_name(self):
        return 'biz_{0}'.format(random.random())

    @pytest.fixture(scope='class')
    def biz_src_resp(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(
            yelp_namespace,
            biz_src_name
        ).topic.source

    @pytest.fixture(scope='class')
    def usr_src_name(self):
        return 'user_{0}'.format(random.random())

    @pytest.fixture(scope='class')
    def cta_src_name(self):
        return 'cta_{0}'.format(random.random())

    @pytest.fixture(scope='class')
    def biz_topic_resp(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name).topic

    @property
    def source_owner_email(self):
        return 'bam+test@yelp.com'

    def _get_client(self):
        """This is a method instead of a property.  Pytest was accessing this
        attribute before setting up fixtures, resulting in this code failing
        since the clientlib hadn't yet been reconfigured to access the
        schematizer container.
        """
        return get_config().schematizer_client

    def _register_avro_schema(self, namespace, source, schema_json=None, **overrides):
        schema_json = schema_json or {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'doc': 'test',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'foo'}]
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

    def _get_schema_by_id(self, schema_id):
        return self._get_client().schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()

    def _assert_schema_values(self, actual, expected_resp):
        attrs = ('schema_id', 'base_schema_id', 'status', 'primary_keys', 'note',
                 'created_at', 'updated_at')
        self._assert_equal_multi_attrs(actual, expected_resp, *attrs)
        assert actual.schema_json == simplejson.loads(expected_resp.schema)
        self._assert_topic_values(actual.topic, expected_resp.topic)

    def _assert_schema_element_values(self, actual, expected_resp):
        assert len(actual) == len(expected_resp)
        attrs = ('note', 'schema_id', 'element_type', 'key', 'id')

        for i in range(0, len(actual)):
            self._assert_equal_multi_attrs(actual[i], expected_resp[i], *attrs)

    def _assert_avro_schemas_equal(self, actual, expected_resp):
        attrs = ('schema_id', 'base_schema_id', 'status', 'primary_keys', 'note',
                 'created_at', 'updated_at')
        self._assert_equal_multi_attrs(actual, expected_resp, *attrs)
        assert actual.schema_json == expected_resp.schema_json
        self._assert_topic_values(actual.topic, expected_resp.topic)

    def _assert_topic_values(self, actual, expected_resp):
        attrs = ('topic_id', 'name', 'contains_pii', 'primary_keys', 'created_at', 'updated_at')
        self._assert_equal_multi_attrs(actual, expected_resp, *attrs)
        self._assert_source_values(actual.source, expected_resp.source)

    def _assert_source_values(self, actual, expected_resp):
        attrs = ('source_id', 'name', 'owner_email')
        self._assert_equal_multi_attrs(actual, expected_resp, *attrs)
        assert actual.namespace.namespace_id == expected_resp.namespace.namespace_id

    def _assert_equal_multi_attrs(self, actual, expected, *attrs):
        for attr in attrs:
            assert getattr(actual, attr) == getattr(expected, attr)


class TestAPIClient(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema(self, yelp_namespace, biz_src_name, containers):
        return self._register_avro_schema(yelp_namespace, biz_src_name)

    def test_retry_api_call(self, schematizer, biz_schema):
        with mock.patch.object(
            schematizer,
            '_get_api_result',
            side_effect=[ConnectionError, ConnectionError, None]
        ) as api_spy:
            schematizer._call_api(
                api=schematizer._client.schemas.get_schema_by_id,
                params={'schema_id': biz_schema.schema_id}
            )
            assert api_spy.call_count == 3


class TestGetSchemaById(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name)

    def test_get_non_cached_schema_by_id(self, schematizer, biz_schema):
        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schema_by_id'
        ) as api_spy:
            actual = schematizer.get_schema_by_id(biz_schema.schema_id)
            self._assert_schema_values(actual, biz_schema)
            assert api_spy.call_count == 1

    def test_get_cached_schema_by_id(self, schematizer, biz_schema):
        schematizer.get_schema_by_id(biz_schema.schema_id)

        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schema_by_id'
        ) as schema_api_spy, self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_schema_by_id(biz_schema.schema_id)
            self._assert_schema_values(actual, biz_schema)
            assert schema_api_spy.call_count == 0
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0


class TestGetSchemaElementsBySchemaId(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name)

    def test_get_schema_elements_by_schema_id(self, schematizer, biz_schema):
        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schema_elements_by_schema_id'
        ) as api_spy:
            actual = schematizer.get_schema_elements_by_schema_id(
                biz_schema.schema_id
            )
            for element in actual:
                assert element.schema_id == biz_schema.schema_id
            assert api_spy.call_count == 1


class TestGetSchemasCreatedAfterDate(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def sorted_schemas(self, yelp_namespace, biz_src_name):
        biz_schema = self._register_avro_schema(
            namespace=yelp_namespace,
            source=biz_src_name
        )

        time.sleep(1)
        schema_json = {
            'type': 'record',
            'name': biz_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'simple'}]
        }
        simple_schema = self._register_avro_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_json=schema_json
        )

        time.sleep(1)
        schema_json = {
            'type': 'record',
            'name': biz_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'baz'}]
        }
        baz_schema = self._register_avro_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_json=schema_json
        )

        return [biz_schema, simple_schema, baz_schema]

    def test_get_schemas_created_after_date_filter_by_min_id(
        self,
        sorted_schemas,
        schematizer
    ):
        created_at = sorted_schemas[0].created_at
        creation_timestamp = long(
            (created_at - datetime.utcfromtimestamp(0)).total_seconds()
        )
        min_id = sorted_schemas[1].schema_id
        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schemas_created_after'
        ) as schemas_api_spy:
            schemas = schematizer.get_schemas_created_after_date(
                created_after=creation_timestamp,
                min_id=min_id
            )
            for schema in schemas:
                assert schema.schema_id >= min_id
            # By default, Schematizer will fetch only 10 schemas at a time.
            assert schemas_api_spy.call_count == len(schemas) / 10 + 1

    def test_get_schemas_created_after_with_page_size(
        self,
        sorted_schemas,
        schematizer
    ):
        created_at = sorted_schemas[0].created_at
        creation_timestamp = long(
            (created_at - datetime.utcfromtimestamp(0)).total_seconds()
        )

        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schemas_created_after'
        ) as schemas_api_spy:
            schemas = schematizer.get_schemas_created_after_date(
                created_after=creation_timestamp,
                min_id=1,
                page_size=1
            )
            # Since page size is 1, we would need to call api endpoint
            # len(schemas) + 1 times before we get a page with schemas less
            # than the page size.
            assert schemas_api_spy.call_count == len(schemas) + 1

    def test_get_schemas_created_after_date(self, schematizer):
        created_after_str = "2015-01-01T19:10:26"
        created_after = datetime.strptime(created_after_str,
                                          '%Y-%m-%dT%H:%M:%S')
        creation_timestamp = long((created_after -
                                   datetime.utcfromtimestamp(0)).total_seconds())
        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schemas_created_after'
        ) as api_spy:
            schemas = schematizer.get_schemas_created_after_date(
                creation_timestamp
            )
            for schema in schemas:
                assert schema.created_at >= created_after
            # By default, Schematizer will fetch only 10 schemas at a time
            assert api_spy.call_count == len(schemas) / 10 + 1

    def test_get_schemas_created_after_date_filter(self, schematizer):
        created_after_str = "2015-01-01T19:10:26"
        created_after = datetime.strptime(created_after_str,
                                          '%Y-%m-%dT%H:%M:%S')
        creation_timestamp = long((created_after -
                                   datetime.utcfromtimestamp(0)).total_seconds())

        created_after_str2 = "2016-06-10T19:10:26"
        created_after2 = datetime.strptime(created_after_str2,
                                           '%Y-%m-%dT%H:%M:%S')
        creation_timestamp2 = long((created_after2 -
                                    datetime.utcfromtimestamp(0)).total_seconds())
        schemas = schematizer.get_schemas_created_after_date(
            creation_timestamp
        )
        schemas_later = schematizer.get_schemas_created_after_date(
            creation_timestamp2
        )
        assert len(schemas) >= len(schemas_later)

    def test_get_schemas_created_after_date_cached(self, schematizer):
        created_after_str = "2015-01-01T19:10:26"
        created_after = datetime.strptime(created_after_str,
                                          '%Y-%m-%dT%H:%M:%S')
        creation_timestamp = long((created_after -
                                   datetime.utcfromtimestamp(0)).total_seconds())
        schemas = schematizer.get_schemas_created_after_date(creation_timestamp)
        # Assert each element was cached properly
        for schema in schemas:
            actual = schematizer.get_schema_by_id(schema.schema_id)
            self._assert_avro_schemas_equal(actual, schema)


class TestGetSchemasByTopic(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name)

    def test_get_schemas_by_topic(self, schematizer, biz_schema):
        with self.attach_spy_on_api(
            schematizer._client.topics,
            'list_schemas_by_topic_name'
        ) as api_spy:
            topic_name = biz_schema.topic.name
            actual = schematizer.get_schemas_by_topic(topic_name)
            found_schema = False
            for schema in actual:
                # Find the schema in the list of schemas, and then check it's
                # values against our schema
                if schema.schema_id == biz_schema.schema_id:
                    found_schema = True
                    self._assert_schema_values(schema, biz_schema)
                    break
            assert found_schema
            assert api_spy.call_count == 1


class TestGetNamespaces(SchematizerClientTestBase):

    def test_get_namespaces(self, schematizer, biz_src_resp):
        actual = schematizer.get_namespaces()
        partial = Namespace(
            namespace_id=biz_src_resp.namespace.namespace_id,
            name=biz_src_resp.namespace.name
        )
        assert partial in actual


class TestGetSchemaBySchemaJson(SchematizerClientTestBase):

    @pytest.fixture
    def schema_json(self, yelp_namespace, biz_src_name):
        return {
            'type': 'record',
            'name': biz_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'biz_id'}]
        }

    @pytest.fixture
    def schema_str(self, schema_json):
        return simplejson.dumps(schema_json)

    def test_get_schema_by_schema_json_returns_none_if_not_cached(
        self,
        schematizer,
        schema_json
    ):
        assert schematizer.get_schema_by_schema_json(schema_json) is None

    def test_get_schema_by_schema_json_returns_cached_schema(
        self,
        schematizer,
        biz_src_name,
        schema_json,
        yelp_namespace
    ):
        schema_one = schematizer.register_schema_from_schema_json(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_json=schema_json,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )

        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'register_schema'
        ) as register_schema_api_spy:
            schema_two = schematizer.get_schema_by_schema_json(schema_json)
            assert register_schema_api_spy.called == 0
            assert schema_one == schema_two


class TestGetTopicByName(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_topic(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name).topic

    def test_get_non_cached_topic_by_name(self, schematizer, biz_topic):
        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as api_spy:
            actual = schematizer.get_topic_by_name(biz_topic.name)
            self._assert_topic_values(actual, biz_topic)
            assert api_spy.call_count == 1

    def test_get_cached_topic_by_name(self, schematizer, biz_topic):
        schematizer.get_topic_by_name(biz_topic.name)

        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_topic_by_name(biz_topic.name)
            self._assert_topic_values(actual, biz_topic)
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0


class TestGetSourceById(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_src(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(
            yelp_namespace,
            biz_src_name
        ).topic.source

    def test_get_non_cached_source_by_id(self, schematizer, biz_src):
        with self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as api_spy:
            actual = schematizer.get_source_by_id(biz_src.source_id)
            self._assert_source_values(actual, biz_src)
            assert api_spy.call_count == 1

    def test_get_cached_source_by_id(self, schematizer, biz_src):
        schematizer.get_source_by_id(biz_src.source_id)

        with self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_source_by_id(biz_src.source_id)
            self._assert_source_values(actual, biz_src)
            assert source_api_spy.call_count == 0


class TestGetSourcesByNamespace(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_src(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(
            yelp_namespace,
            biz_src_name
        ).topic.source

    @pytest.fixture(autouse=True, scope='class')
    def usr_src(self, yelp_namespace, usr_src_name):
        return self._register_avro_schema(
            yelp_namespace,
            usr_src_name
        ).topic.source

    @pytest.fixture(autouse=True, scope='class')
    def cta_src(self, aux_namespace, cta_src_name):
        return self._register_avro_schema(
            aux_namespace,
            cta_src_name
        ).topic.source

    def test_get_sources_in_yelp_namespace(
        self,
        schematizer,
        yelp_namespace,
        biz_src,
        usr_src
    ):
        actual = schematizer.get_sources_by_namespace(yelp_namespace)

        sorted_expected = sorted([biz_src, usr_src], key=lambda o: o.source_id)
        sorted_actual = sorted(actual, key=lambda o: o.source_id)
        for actual_src, expected_resp in zip(sorted_actual, sorted_expected):
            self._assert_source_values(actual_src, expected_resp)

    def test_get_sources_of_bad_namespace(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_sources_by_namespace('bad_namespace')
        assert e.value.response.status_code == 404

    def test_sources_should_be_cached(self, schematizer, yelp_namespace):
        sources = schematizer.get_sources_by_namespace(yelp_namespace)
        with self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_source_by_id(sources[0].source_id)
            assert actual == sources[0]
            assert source_api_spy.call_count == 0


class TestGetTopicsBySourceId(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_topic(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name).topic

    @pytest.fixture(autouse=True, scope='class')
    def pii_biz_topic(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(
            yelp_namespace,
            biz_src_name,
            contains_pii=True
        ).topic

    def test_get_topics_of_biz_source(self, schematizer, biz_topic, pii_biz_topic):
        actual = schematizer.get_topics_by_source_id(biz_topic.source.source_id)

        sorted_expected = sorted(
            [biz_topic, pii_biz_topic],
            key=lambda o: o.topic_id
        )
        sorted_actual = sorted(actual, key=lambda o: o.topic_id)
        for actual_topic, expected_resp in zip(sorted_actual, sorted_expected):
            self._assert_topic_values(actual_topic, expected_resp)

    def test_get_topics_of_bad_source_id(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_topics_by_source_id(0)
        assert e.value.response.status_code == 404

    def test_topics_should_be_cached(self, schematizer, biz_topic):
        topics = schematizer.get_topics_by_source_id(biz_topic.source.source_id)
        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_topic_by_name(topics[0].name)
            assert actual == topics[0]
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0


class TestGetLatestTopicBySourceId(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_topic(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(
            yelp_namespace,
            biz_src_name
        ).topic

    def test_get_latest_topic_of_biz_source(self, schematizer, biz_topic):
        actual = schematizer.get_latest_topic_by_source_id(biz_topic.source.source_id)
        expected = biz_topic
        self._assert_topic_values(actual, expected)

    def test_get_latest_topic_of_bad_source(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_latest_topic_by_source_id(0)
        assert e.value.response.status_code == 404


class TestGetLatestSchemaByTopicName(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name)

    @pytest.fixture(autouse=True, scope='class')
    def biz_topic(self, biz_schema):
        return biz_schema.topic

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema_two(self, biz_schema):
        new_schema = simplejson.loads(biz_schema.schema)
        new_schema['fields'].append({'type': 'int', 'doc': 'test', 'name': 'bar', 'default': 0})
        return self._register_avro_schema(
            namespace=biz_schema.topic.source.namespace.name,
            source=biz_schema.topic.source.name,
            schema=simplejson.dumps(new_schema)
        )

    def test_get_latest_schema_of_biz_topic(
        self,
        schematizer,
        biz_topic,
        biz_schema_two
    ):
        actual = schematizer.get_latest_schema_by_topic_name(biz_topic.name)
        self._assert_schema_values(actual, biz_schema_two)

    def test_latest_schema_of_bad_topic(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_latest_schema_by_topic_name('bad_topic')
        assert e.value.response.status_code == 404

    def test_latest_schema_should_be_cached(self, schematizer, biz_topic):
        latest_schema = schematizer.get_latest_schema_by_topic_name(biz_topic.name)
        with self.attach_spy_on_api(
            schematizer._client.schemas,
            'get_schema_by_id'
        ) as schema_api_spy, self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_schema_by_id(latest_schema.schema_id)
            assert actual == latest_schema
            assert schema_api_spy.call_count == 0
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0


class TestRegisterSchema(SchematizerClientTestBase):

    @pytest.fixture
    def schema_json(self, yelp_namespace, biz_src_name):
        return {
            'type': 'record',
            'name': biz_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'biz_id'}]
        }

    @pytest.fixture
    def schema_str(self, schema_json):
        return simplejson.dumps(schema_json)

    def test_register_schema(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str
    ):
        actual = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        expected = self._get_schema_by_id(actual.schema_id)
        self._assert_schema_values(actual, expected)

    def test_register_schema_with_schema_json(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_json
    ):
        actual = schematizer.register_schema_from_schema_json(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_json=schema_json,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        expected = self._get_schema_by_id(actual.schema_id)
        self._assert_schema_values(actual, expected)

    def test_register_schema_with_base_schema(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str
    ):
        actual = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=10
        )
        expected = self._get_schema_by_id(actual.schema_id)
        self._assert_schema_values(actual, expected)

    def test_register_same_schema_twice(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str
    ):
        schema_one = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        schema_two = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        assert schema_one == schema_two

    def test_register_same_schema_with_diff_base_schema(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str
    ):
        schema_one = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=10
        )
        schema_two = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            base_schema_id=20
        )
        self._assert_two_schemas_have_diff_topics(schema_one, schema_two)
        assert schema_one.topic.source == schema_two.topic.source

    def test_register_same_schema_with_diff_pii(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str
    ):
        schema_one = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        schema_two = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=True
        )
        self._assert_two_schemas_have_diff_topics(schema_one, schema_two)
        assert not schema_one.topic.contains_pii
        assert schema_two.topic.contains_pii
        assert schema_one.topic.source == schema_two.topic.source

    def test_register_same_schema_with_diff_source(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str
    ):
        another_src = 'biz_user'
        schema_one = schematizer.register_schema(
            namespace=yelp_namespace,
            source=biz_src_name,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        schema_two = schematizer.register_schema(
            namespace=yelp_namespace,
            source=another_src,
            schema_str=schema_str,
            source_owner_email=self.source_owner_email,
            contains_pii=False
        )
        self._assert_two_schemas_have_diff_topics(schema_one, schema_two)

        src_one = schema_one.topic.source
        src_two = schema_two.topic.source
        assert src_one.name == biz_src_name
        assert src_two.name == another_src
        assert src_one.source_id != src_two.source_id
        assert src_one.namespace == src_two.namespace

    def _assert_two_schemas_have_diff_topics(self, schema_one, schema_two):
        assert schema_one.schema_id != schema_two.schema_id
        assert schema_one.schema_json == schema_two.schema_json

        assert schema_one.topic.topic_id != schema_two.topic.topic_id
        assert schema_one.topic.name != schema_two.topic.name


class TestRegisterSchemaFromMySQL(SchematizerClientTestBase):

    @property
    def old_create_biz_table_stmt(self):
        return 'create table biz(id int(11) not null);'

    @property
    def alter_biz_table_stmt(self):
        return 'alter table biz add column name varchar(8);'

    @property
    def new_create_biz_table_stmt(self):
        return 'create table biz(id int(11) not null, name varchar(8));'

    @pytest.fixture
    def avro_schema_of_new_biz_table(self, biz_src_name):
        return {
            'type': 'record',
            'name': biz_src_name,
            'namespace': '',
            'doc': 'test',
            'fields': [
                {'name': 'id', 'doc': 'test', 'type': 'int'},
                {'name': 'name', 'doc': 'test',
                 'type': ['null', 'string'], 'maxlen': '8', 'default': None}
            ]
        }

    def test_register_for_new_table(self, schematizer, yelp_namespace, biz_src_name):
        actual = schematizer.register_schema_from_mysql_stmts(
            namespace=yelp_namespace,
            source=biz_src_name,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=self.new_create_biz_table_stmt
        )
        expected = self._get_schema_by_id(actual.schema_id)
        self._assert_schema_values(actual, expected)

    def test_register_for_updated_existing_table(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name
    ):
        actual = schematizer.register_schema_from_mysql_stmts(
            namespace=yelp_namespace,
            source=biz_src_name,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=self.new_create_biz_table_stmt,
            old_create_table_stmt=self.old_create_biz_table_stmt,
            alter_table_stmt=self.alter_biz_table_stmt
        )
        expected = self._get_schema_by_id(actual.schema_id)
        self._assert_schema_values(actual, expected)

    def test_register_same_schema_with_diff_pii(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name
    ):
        non_pii_schema = schematizer.register_schema_from_mysql_stmts(
            namespace=yelp_namespace,
            source=biz_src_name,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=self.new_create_biz_table_stmt
        )
        pii_schema = schematizer.register_schema_from_mysql_stmts(
            namespace=yelp_namespace,
            source=biz_src_name,
            source_owner_email=self.source_owner_email,
            contains_pii=True,
            new_create_table_stmt=self.new_create_biz_table_stmt
        )

        assert non_pii_schema.schema_id != pii_schema.schema_id
        assert non_pii_schema.schema_json == pii_schema.schema_json
        assert non_pii_schema.base_schema_id == pii_schema.base_schema_id

        assert non_pii_schema.topic.topic_id != pii_schema.topic.topic_id
        assert not non_pii_schema.topic.contains_pii
        assert pii_schema.topic.contains_pii

        assert non_pii_schema.topic.source == non_pii_schema.topic.source

    def test_register_schema_with_primary_keys(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name
    ):
        schema_sql = ('create table biz(id int(11) not null, name varchar(8), '
                      'primary key (id));')
        actual = schematizer.register_schema_from_mysql_stmts(
            namespace=yelp_namespace,
            source=biz_src_name,
            source_owner_email=self.source_owner_email,
            contains_pii=False,
            new_create_table_stmt=schema_sql
        )
        expected = self._get_schema_by_id(actual.schema_id)
        self._assert_schema_values(actual, expected)


class TestGetTopicsByCriteria(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def yelp_biz_topic(self, yelp_namespace, biz_src_name):
        return self._register_avro_schema(yelp_namespace, biz_src_name).topic

    @pytest.fixture(autouse=True, scope='class')
    def yelp_usr_topic(self, yelp_namespace, usr_src_name):
        return self._register_avro_schema(yelp_namespace, usr_src_name).topic

    @pytest.fixture(autouse=True, scope='class')
    def aux_biz_topic(self, aux_namespace, biz_src_name):
        return self._register_avro_schema(aux_namespace, biz_src_name).topic

    @pytest.fixture
    def yelp_topics(self, yelp_biz_topic, yelp_usr_topic):
        return [yelp_biz_topic, yelp_usr_topic]

    @pytest.fixture
    def biz_src_topics(self, yelp_biz_topic, aux_biz_topic):
        return [yelp_biz_topic, aux_biz_topic]

    def test_get_topics_of_yelp_namespace(
        self,
        schematizer,
        yelp_namespace,
        yelp_topics,
    ):
        actual = schematizer.get_topics_by_criteria(
            namespace_name=yelp_namespace
        )
        self._assert_topics_values(actual, expected_topics=yelp_topics)

    def test_get_topics_of_biz_source(
        self,
        schematizer,
        biz_src_name,
        biz_src_topics,
    ):
        actual = schematizer.get_topics_by_criteria(
            source_name=biz_src_name
        )
        self._assert_topics_values(actual, expected_topics=biz_src_topics)

    def _assert_topics_values(self, actual, expected_topics):
        sorted_expected = sorted(expected_topics, key=lambda o: o.topic_id)
        sorted_actual = sorted(actual, key=lambda o: o.topic_id)
        for actual_topic, expected_resp in zip(sorted_actual, sorted_expected):
            self._assert_topic_values(actual_topic, expected_resp)

    def test_get_topics_of_bad_namesapce_name(self, schematizer):
        actual = schematizer.get_topics_by_criteria(namespace_name='foo')
        assert actual == []

    def test_get_topics_of_bad_source_name(self, schematizer):
        actual = schematizer.get_topics_by_criteria(source_name='foo')
        assert actual == []

    def test_get_topics_with_future_created_after_timestamp(self, schematizer):
        actual = schematizer.get_topics_by_criteria(
            created_after=int(time.time() + 60)
        )
        assert actual == []

    def test_topics_should_be_cached(self, schematizer, yelp_namespace):
        topics = schematizer.get_topics_by_criteria(
            namespace_name=yelp_namespace
        )
        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topic_by_topic_name'
        ) as topic_api_spy, self.attach_spy_on_api(
            schematizer._client.sources,
            'get_source_by_id'
        ) as source_api_spy:
            actual = schematizer.get_topic_by_name(topics[0].name)
            assert actual == topics[0]
            assert topic_api_spy.call_count == 0
            assert source_api_spy.call_count == 0

    def test_topic_page_size(
        self,
        schematizer,
        yelp_namespace,
        yelp_topics
    ):
        with self.attach_spy_on_api(
            schematizer._client.topics,
            'get_topics_by_criteria'
        ) as topic_api_spy:
            actual = schematizer.get_topics_by_criteria(
                namespace_name=yelp_namespace,
                page_size=1
            )
            self._assert_topics_values(actual, expected_topics=yelp_topics)
            # Since we have a page size of 1, we should need to fetch len(yelp_topics) + 1 pages
            # before we get a page with #items less than our page_size (0 items)
            # Thus, we should call the api len(yelp_topic) + 1 times
            assert topic_api_spy.call_count == len(yelp_topics) + 1

    def test_topic_min_id(
        self,
        schematizer,
        yelp_namespace,
        yelp_topics
    ):
        sorted_yelp_topics = sorted(yelp_topics, key=lambda topic: topic.topic_id)
        actual = schematizer.get_topics_by_criteria(
            namespace_name=yelp_namespace,
            min_id=sorted_yelp_topics[0].topic_id + 1
        )
        assert len(actual) == len(sorted_yelp_topics) - 1
        self._assert_topics_values(actual, expected_topics=sorted_yelp_topics[1:])


class TestIsAvroSchemaCompatible(SchematizerClientTestBase):

    @pytest.fixture(scope='class')
    def schema_json(self, yelp_namespace, biz_src_name):
        return {
            'type': 'record',
            'name': biz_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [
                {'type': 'int', 'doc': 'test', 'name': 'biz_id'}
            ]
        }

    @pytest.fixture
    def schema_json_incompatible(self, yelp_namespace, biz_src_name):
        return {
            'type': 'record',
            'name': biz_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [
                {'type': 'int', 'doc': 'test', 'name': 'biz_id'},
                {'type': 'int', 'doc': 'test', 'name': 'new_field'}
            ]
        }

    @pytest.fixture(scope='class')
    def schema_str(self, schema_json):
        return simplejson.dumps(schema_json)

    @pytest.fixture
    def schema_str_incompatible(self, schema_json_incompatible):
        return simplejson.dumps(schema_json_incompatible)

    @pytest.fixture(autouse=True, scope='class')
    def biz_schema(self, yelp_namespace, biz_src_name, schema_str):
        return self._register_avro_schema(
            yelp_namespace,
            biz_src_name,
            schema=schema_str
        )

    def test_is_avro_schema_compatible(
        self,
        schematizer,
        yelp_namespace,
        biz_src_name,
        schema_str,
        schema_str_incompatible
    ):
        assert schematizer.is_avro_schema_compatible(
            avro_schema_str=schema_str,
            namespace_name=yelp_namespace,
            source_name=biz_src_name
        )
        assert not schematizer.is_avro_schema_compatible(
            avro_schema_str=schema_str_incompatible,
            namespace_name=yelp_namespace,
            source_name=biz_src_name
        )


class TestFilterTopicsByPkeys(SchematizerClientTestBase):

    @pytest.fixture(autouse=True, scope='class')
    def pk_topic_resp(self, yelp_namespace, usr_src_name):
        pk_schema_json = {
            'type': 'record',
            'name': usr_src_name,
            'namespace': yelp_namespace,
            'doc': 'test',
            'fields': [
                {'type': 'int', 'doc': 'test', 'name': 'id', 'pkey': 1},
                {'type': 'int', 'doc': 'test', 'name': 'data'}
            ],
            'pkey': ['id']
        }
        return self._register_avro_schema(
            yelp_namespace,
            usr_src_name,
            schema=simplejson.dumps(pk_schema_json)
        ).topic

    def test_filter_topics_by_pkeys(
        self,
        schematizer,
        biz_topic_resp,
        pk_topic_resp
    ):
        topics = [
            biz_topic_resp.name,
            pk_topic_resp.name
        ]
        assert schematizer.filter_topics_by_pkeys(topics) == [pk_topic_resp.name]


class RegistrationTestBase(SchematizerClientTestBase):

    @pytest.fixture(scope="class")
    def dw_data_target_resp(self):
        return self._create_data_target()

    def _create_data_target(self):
        post_body = {
            'target_type': 'redshift_{}'.format(random.random()),
            'destination': 'dwv1.yelpcorp.com.{}'.format(random.random())
        }
        return self._get_client().data_targets.create_data_target(
            body=post_body
        ).result()

    @pytest.fixture(scope="class")
    def dw_con_group_resp(self, dw_data_target_resp):
        return self._create_consumer_group(dw_data_target_resp.data_target_id)

    def _create_consumer_group(self, data_target_id):
        return self._get_client().data_targets.create_consumer_group(
            data_target_id=data_target_id,
            body={'group_name': 'dw_{}'.format(random.random())}
        ).result()

    @pytest.fixture(scope="class")
    def dw_con_group_data_src_resp(self, dw_con_group_resp, biz_src_resp):
        return self._create_consumer_group_data_src(
            consumer_group_id=dw_con_group_resp.consumer_group_id,
            data_src_type='Source',
            data_src_id=biz_src_resp.source_id
        )

    def _create_consumer_group_data_src(
        self,
        consumer_group_id,
        data_src_type,
        data_src_id
    ):
        return self._get_client().consumer_groups.create_consumer_group_data_source(
            consumer_group_id=consumer_group_id,
            body={
                'data_source_type': data_src_type,
                'data_source_id': data_src_id
            }
        ).result()

    def _assert_data_target_values(self, actual, expected_resp):
        attrs = ('data_target_id', 'target_type', 'destination')
        self._assert_equal_multi_attrs(actual, expected_resp, *attrs)

    def _assert_consumer_group_values(self, actual, expected_resp):
        attrs = ('consumer_group_id', 'group_name')
        self._assert_equal_multi_attrs(actual, expected_resp, *attrs)
        self._assert_data_target_values(
            actual.data_target,
            expected_resp.data_target
        )


class TestCreateDataTarget(RegistrationTestBase):

    @property
    def random_target_type(self):
        return 'random_type'

    @property
    def random_destination(self):
        return 'random.destination'

    def test_create_data_target(self, schematizer):
        actual = schematizer.create_data_target(
            target_type=self.random_target_type,
            destination=self.random_destination
        )
        expected_resp = self._get_data_target_resp(actual.data_target_id)
        self._assert_data_target_values(actual, expected_resp)
        assert actual.target_type == self.random_target_type
        assert actual.destination == self.random_destination

    def test_invalid_empty_target_type(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_data_target(
                target_type='',
                destination=self.random_destination
            )
        assert e.value.response.status_code == 400

    def test_invalid_empty_destination(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_data_target(
                target_type=self.random_target_type,
                destination=''
            )
        assert e.value.response.status_code == 400

    def _get_data_target_resp(self, data_target_id):
        return self._get_client().data_targets.get_data_target_by_id(
            data_target_id=data_target_id
        ).result()


class TestGetDataTargetById(RegistrationTestBase):

    def test_get_non_cached_data_target(self, schematizer, dw_data_target_resp):
        with self.attach_spy_on_api(
            schematizer._client.data_targets,
            'get_data_target_by_id'
        ) as api_spy:
            actual = schematizer.get_data_target_by_id(
                dw_data_target_resp.data_target_id
            )
            self._assert_data_target_values(actual, dw_data_target_resp)
            assert api_spy.call_count == 1

    def test_get_cached_data_target(self, schematizer, dw_data_target_resp):
        schematizer.get_data_target_by_id(dw_data_target_resp.data_target_id)

        with self.attach_spy_on_api(
            schematizer._client.data_targets,
            'get_data_target_by_id'
        ) as data_target_api_spy:
            actual = schematizer.get_data_target_by_id(
                dw_data_target_resp.data_target_id
            )
            self._assert_data_target_values(actual, dw_data_target_resp)
            assert data_target_api_spy.call_count == 0

    def test_non_existing_data_target_id(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_data_target_by_id(data_target_id=0)
        assert e.value.response.status_code == 404


class TestCreateConsumerGroup(RegistrationTestBase):

    @pytest.fixture
    def random_group_name(self):
        return 'group_{}'.format(random.random())

    def test_create_consumer_group(
        self,
        schematizer,
        dw_data_target_resp,
        random_group_name
    ):
        actual = schematizer.create_consumer_group(
            group_name=random_group_name,
            data_target_id=dw_data_target_resp.data_target_id
        )
        expected_resp = self._get_consumer_group_resp(
            actual.consumer_group_id
        )
        self._assert_consumer_group_values(actual, expected_resp)
        assert actual.group_name == random_group_name

    def test_invalid_empty_group_name(self, schematizer, dw_data_target_resp):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_consumer_group(
                group_name='',
                data_target_id=dw_data_target_resp.data_target_id
            )
        assert e.value.response.status_code == 400

    def test_duplicate_group_name(
        self,
        schematizer,
        dw_data_target_resp,
        random_group_name
    ):
        schematizer.create_consumer_group(
            group_name=random_group_name,
            data_target_id=dw_data_target_resp.data_target_id
        )
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_consumer_group(
                group_name=random_group_name,
                data_target_id=dw_data_target_resp.data_target_id
            )
        assert e.value.response.status_code == 400

    def test_non_existing_data_target(self, schematizer, random_group_name):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_consumer_group(
                group_name=random_group_name,
                data_target_id=0
            )
        assert e.value.response.status_code == 404

    def _get_consumer_group_resp(self, consumer_group_id):
        return self._get_client().consumer_groups.get_consumer_group_by_id(
            consumer_group_id=consumer_group_id
        ).result()


class TestGetConsumerGroupById(RegistrationTestBase):

    def test_get_non_cached_consumer_group(self, schematizer, dw_con_group_resp):
        with self.attach_spy_on_api(
            schematizer._client.consumer_groups,
            'get_consumer_group_by_id'
        ) as api_spy:
            actual = schematizer.get_consumer_group_by_id(
                dw_con_group_resp.consumer_group_id
            )
            self._assert_consumer_group_values(actual, dw_con_group_resp)
            assert api_spy.call_count == 1

    def test_get_cached_consumer_group(self, schematizer, dw_con_group_resp):
        schematizer.get_consumer_group_by_id(
            dw_con_group_resp.consumer_group_id
        )

        with self.attach_spy_on_api(
            schematizer._client.consumer_groups,
            'get_consumer_group_by_id'
        ) as consumer_group_api_spy:
            actual = schematizer.get_consumer_group_by_id(
                dw_con_group_resp.consumer_group_id
            )
            self._assert_consumer_group_values(actual, dw_con_group_resp)
            assert consumer_group_api_spy.call_count == 0

    def test_non_existing_consumer_group_id(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_consumer_group_by_id(consumer_group_id=0)
        assert e.value.response.status_code == 404


class TestCreateConsumerGroupDataSource(RegistrationTestBase):

    def test_create_consumer_group_data_source(
        self,
        schematizer,
        dw_con_group_resp,
        biz_src_resp
    ):
        actual = schematizer.create_consumer_group_data_source(
            consumer_group_id=dw_con_group_resp.consumer_group_id,
            data_source_type=DataSourceTypeEnum.Source,
            data_source_id=biz_src_resp.source_id
        )
        assert actual.consumer_group_id == dw_con_group_resp.consumer_group_id
        assert actual.data_source_type == DataSourceTypeEnum.Source
        assert actual.data_source_id == biz_src_resp.source_id

    def test_non_existing_consumer_group(self, schematizer, biz_src_resp):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_consumer_group_data_source(
                consumer_group_id=0,
                data_source_type=DataSourceTypeEnum.Source,
                data_source_id=biz_src_resp.source_id
            )
        assert e.value.response.status_code == 404

    def test_non_existing_data_source(self, schematizer, dw_con_group_resp):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.create_consumer_group_data_source(
                consumer_group_id=dw_con_group_resp.consumer_group_id,
                data_source_type=DataSourceTypeEnum.Source,
                data_source_id=0
            )
        assert e.value.response.status_code == 404


class TestGetTopicsByDataTargetId(RegistrationTestBase):

    def test_data_target_with_topics(
        self,
        schematizer,
        dw_data_target_resp,
        dw_con_group_data_src_resp,
        biz_src_resp,
        biz_topic_resp
    ):
        actual = schematizer.get_topics_by_data_target_id(
            dw_data_target_resp.data_target_id
        )
        for actual_topic, expected_resp in zip(actual, [biz_topic_resp]):
            self._assert_topic_values(actual_topic, expected_resp)

    def test_data_target_with_no_topic(self, schematizer):
        # no data source associated to the consumer group
        random_data_target = self._create_data_target()
        self._create_consumer_group(random_data_target.data_target_id)

        actual = schematizer.get_topics_by_data_target_id(
            random_data_target.data_target_id
        )
        assert actual == []

    def test_non_existing_data_target(self, schematizer):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_topics_by_data_target_id(data_target_id=0)
        assert e.value.response.status_code == 404


class TestGetSchemaMigration(SchematizerClientTestBase):

    @pytest.fixture
    def new_schema(self):
        return {
            'type': 'record',
            'name': 'schema_a',
            'doc': 'test',
            'namespace': 'test_namespace',
            'fields': [{'type': 'int', 'doc': 'test', 'name': 'test_id'}]
        }

    @pytest.fixture(params=[True, False])
    def old_schema(self, request, new_schema):
        return new_schema if request.param else None

    def test_normal_schema_migration(
        self,
        schematizer,
        new_schema,
        old_schema
    ):
        with self.attach_spy_on_api(
            schematizer._client.schema_migrations,
            'get_schema_migration'
        ) as api_spy:
            actual = schematizer.get_schema_migration(
                new_schema=new_schema,
                target_schema_type=TargetSchemaTypeEnum.redshift,
                old_schema=old_schema
            )
            assert isinstance(actual, list)
            assert len(actual) > 0
            assert api_spy.call_count == 1

    def test_invalid_schema(
        self,
        schematizer,
        new_schema
    ):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer._call_api(
                api=schematizer._client.schema_migrations.get_schema_migration,
                request_body={
                    'new_schema': '{}}',
                    'target_schema_type': TargetSchemaTypeEnum.redshift.name,
                }
            )
        assert e.value.response.status_code == 422

    def test_unsupported_schema_migration(
        self,
        schematizer,
        new_schema
    ):
        with pytest.raises(swaggerpy_exc.HTTPError) as e:
            schematizer.get_schema_migration(
                new_schema=new_schema,
                target_schema_type=TargetSchemaTypeEnum.unsupported
            )
        assert e.value.response.status_code == 501
