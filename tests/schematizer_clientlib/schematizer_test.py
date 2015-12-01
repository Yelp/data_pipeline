# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random
import time

import mock
import pytest
import simplejson
from swaggerpy import exception as swaggerpy_exc

from data_pipeline.config import get_config
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
    def usr_src_name(self):
        return 'user_{0}'.format(random.random())

    @pytest.fixture(scope='class')
    def cta_src_name(self):
        return 'cta_{0}'.format(random.random())

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

    def _register_avro_schema(self, namespace, source, **overrides):
        schema_json = {
            'type': 'record',
            'name': source,
            'namespace': namespace,
            'fields': [{'type': 'int', 'name': 'foo'}]
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
        assert actual.schema_id == expected_resp.schema_id
        assert actual.schema_json == simplejson.loads(expected_resp.schema)
        self._assert_topic_values(actual.topic, expected_resp.topic)
        assert actual.base_schema_id == expected_resp.base_schema_id
        assert actual.status == expected_resp.status
        assert actual.primary_keys == expected_resp.primary_keys
        assert actual.note == expected_resp.note
        assert actual.created_at == expected_resp.created_at
        assert actual.updated_at == expected_resp.updated_at

    def _assert_topic_values(self, actual, expected_resp):
        assert actual.topic_id == expected_resp.topic_id
        assert actual.name == expected_resp.name
        self._assert_source_values(actual.source, expected_resp.source)
        assert actual.contains_pii == expected_resp.contains_pii
        assert actual.created_at == expected_resp.created_at
        assert actual.updated_at == expected_resp.updated_at

    def _assert_source_values(self, actual, expected_resp):
        assert actual.source_id == expected_resp.source_id
        assert actual.name == expected_resp.name
        assert actual.owner_email == expected_resp.owner_email
        assert actual.namespace.namespace_id == expected_resp.namespace.namespace_id


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
        new_schema['fields'].append({'type': 'int', 'name': 'bar', 'default': 0})
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
        assert e.value.response.status_code == 500

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
            'fields': [{'type': 'int', 'name': 'biz_id'}]
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
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'name',
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

    def test_topics_should_be_cached(self, schematizer):
        topics = schematizer.get_topics_by_criteria()
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
