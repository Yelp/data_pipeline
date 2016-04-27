# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random
import time

import pytest

from data_pipeline.consumer_source import FixedTopics
from data_pipeline.consumer_source import NewTopicOnlyInDataTarget
from data_pipeline.consumer_source import NewTopicsOnlyInFixedNamespaces
from data_pipeline.consumer_source import NewTopicOnlyInSource
from data_pipeline.consumer_source import SingleSchema
from data_pipeline.consumer_source import TopicInDataTarget
from data_pipeline.consumer_source import TopicsInFixedNamespaces
from data_pipeline.consumer_source import TopicInSource
from data_pipeline.schematizer_clientlib.models.data_source_type_enum \
    import DataSourceTypeEnum


class ConsumerSourceTestBase(object):

    @pytest.fixture(scope='module')
    def _register_schema(self, schematizer_client):
        def register_func(namespace, source, avro_schema=None):
            avro_schema = avro_schema or {
                'type': 'record',
                'name': source,
                'namespace': namespace,
                'fields': [{'type': 'int', 'name': 'id'}]
            }
            new_schema = schematizer_client.register_schema_from_schema_json(
                namespace=namespace,
                source=source,
                schema_json=avro_schema,
                source_owner_email='bam+test@yelp.com',
                contains_pii=False
            )
            return new_schema
        return register_func

    def random_name(self, prefix=None):
        suffix = random.random()
        return '{}_{}'.format(prefix, suffix) if prefix else '{}'.format(suffix)

    @pytest.fixture
    def foo_namespace(self):
        return self.random_name('foo_ns')

    @pytest.fixture
    def baz_namespace(self):
        return self.random_name('baz_ns')

    @pytest.fixture
    def foo_src(self):
        return self.random_name('foo_src')

    @pytest.fixture
    def baz_src(self):
        return self.random_name('baz_src')

    @pytest.fixture
    def foo_schema(self, foo_namespace, foo_src, _register_schema):
        return _register_schema(foo_namespace, foo_src)

    @pytest.fixture
    def baz_schema(self, baz_namespace, baz_src, _register_schema):
        return _register_schema(baz_namespace, baz_src)

    @pytest.fixture
    def foo_topic(self, foo_schema):
        return foo_schema.topic.name

    @pytest.fixture
    def baz_topic(self, baz_schema):
        return baz_schema.topic.name

    @pytest.fixture
    def bar_schema(self, foo_namespace, foo_src, _register_schema):
        new_schema = {
            'type': 'record',
            'name': foo_src,
            'namespace': foo_namespace,
            'fields': [{'type': 'string', 'name': 'memo'}]
        }
        return _register_schema(foo_namespace, foo_src, new_schema)

    @pytest.fixture
    def taz_schema(self, baz_namespace, baz_src, _register_schema):
        new_schema = {
            'type': 'record',
            'name': baz_src,
            'namespace': baz_namespace,
            'fields': [{'type': 'string', 'name': 'memo'}]
        }
        return _register_schema(baz_namespace, baz_src, new_schema)

    @pytest.fixture
    def bar_topic(self, bar_schema):
        return bar_schema.topic.name

    @pytest.fixture
    def taz_topic(self, taz_schema):
        return taz_schema.topic.name


@pytest.mark.usefixtures('foo_schema', 'bar_schema')
class FixedTopicsSourceTestBase(ConsumerSourceTestBase):

    def test_happy_case(self, consumer_source, expected):
        assert set(consumer_source.get_topics()) == expected

    def test_get_topics_multiple_times(self, consumer_source, expected):
        assert set(consumer_source.get_topics()) == expected
        assert set(consumer_source.get_topics()) == expected

    def test_get_topics_multiple_times_when_new_topic_created(
        self,
        consumer_source,
        expected,
        foo_namespace,
        foo_src,
        _register_schema
    ):
        assert set(consumer_source.get_topics()) == expected
        new_schema = {
            'type': 'record',
            'name': foo_src,
            'namespace': foo_namespace,
            'fields': [{'type': 'bytes', 'name': 'md5'}]
        }
        _register_schema(foo_namespace, foo_src, new_schema)
        assert set(consumer_source.get_topics()) == expected


class TestSingleTopic(FixedTopicsSourceTestBase):

    @pytest.fixture
    def consumer_source(self, foo_topic):
        return FixedTopics(foo_topic)

    @pytest.fixture
    def expected(self, foo_topic):
        return {foo_topic}


class TestMultiTopics(FixedTopicsSourceTestBase):

    @pytest.fixture
    def consumer_source(self, foo_topic, bar_topic):
        return FixedTopics(foo_topic, bar_topic)

    @pytest.fixture
    def expected(self, foo_topic, bar_topic):
        return {foo_topic, bar_topic}

    def test_invalid_topic(self):
        with pytest.raises(ValueError):
            FixedTopics()
        with pytest.raises(ValueError):
            FixedTopics('')
        with pytest.raises(ValueError):
            FixedTopics('', '')


class TestSingleSchema(FixedTopicsSourceTestBase):

    @pytest.fixture
    def consumer_source(self, foo_schema):
        return SingleSchema(foo_schema.schema_id)

    @pytest.fixture
    def expected(self, foo_topic):
        return {foo_topic}

    def test_invalid_schema(self):
        with pytest.raises(ValueError):
            SingleSchema(schema_id=0)


class NamespaceSrcSetupMixin(ConsumerSourceTestBase):

    @pytest.fixture
    def consumer_source(self, foo_namespace, baz_namespace, consumer_source_cls):
        return consumer_source_cls(namespace_names=[foo_namespace, baz_namespace])

    @pytest.fixture
    def consumer_source_topics(self, foo_topic, baz_topic):
        return [foo_topic, baz_topic]

    @pytest.fixture
    def bad_consumer_source(self, consumer_source_cls):
        return consumer_source_cls(namespace_names=['bad namespace'])


class SourceSrcSetupMixin(ConsumerSourceTestBase):

    @pytest.fixture
    def consumer_source(self, foo_schema, consumer_source_cls):
        src = foo_schema.topic.source
        return consumer_source_cls(
            namespace_name=src.namespace.name,
            source_name=src.name
        )

    @pytest.fixture
    def consumer_source_topics(self, foo_topic):
        return [foo_topic]

    @pytest.fixture
    def bad_consumer_source(self, foo_src, consumer_source_cls):
        return consumer_source_cls(
            namespace_name='bad namespace',
            source_name=foo_src
        )


class DataTargetSetupMixin(ConsumerSourceTestBase):

    @pytest.fixture
    def data_target(self, schematizer_client):
        return schematizer_client.create_data_target(
            target_type='some target type',
            destination='some destination'
        )

    @pytest.fixture
    def consumer_group(self, schematizer_client, data_target):
        return schematizer_client.create_consumer_group(
            group_name=self.random_name('foo_group'),
            data_target_id=data_target.data_target_id
        )

    @pytest.fixture(autouse=True)
    def data_source(self, schematizer_client, foo_schema, consumer_group):
        return schematizer_client.create_consumer_group_data_source(
            consumer_group_id=consumer_group.consumer_group_id,
            data_source_type=DataSourceTypeEnum.Source,
            data_source_id=foo_schema.topic.source.source_id
        )

    @pytest.fixture
    def consumer_source(self, data_target, consumer_source_cls):
        return consumer_source_cls(data_target.data_target_id)

    @pytest.fixture
    def consumer_source_topics(self, foo_topic):
        return [foo_topic]

    @pytest.fixture
    def bad_consumer_source(self, schematizer_client, consumer_source_cls):
        bad_data_target = schematizer_client.create_data_target(
            target_type='bad target type',
            destination='bad destination'
        )
        return consumer_source_cls(bad_data_target.data_target_id)


@pytest.mark.usefixtures('foo_schema', 'baz_schema')
class DynamicTopicSrcTests(ConsumerSourceTestBase):

    def test_get_topics_first_time(self, consumer_source, consumer_source_topics):
        assert consumer_source.get_topics() == consumer_source_topics

    def test_get_topics_multiple_times_with_no_new_topics(
        self,
        consumer_source,
        consumer_source_topics,
    ):
        assert consumer_source.get_topics() == consumer_source_topics
        assert consumer_source.get_topics() == consumer_source_topics

    def test_pick_up_new_topics(
        self,
        consumer_source,
        consumer_source_topics,
        foo_src,
        foo_namespace,
        _register_schema,
    ):
        assert consumer_source.get_topics() == consumer_source_topics
        new_schema = {
            'type': 'record',
            'name': foo_src,
            'namespace': foo_namespace,
            'fields': [{'type': 'bytes', 'name': 'md5'}]
        }
        consumer_source_topics.append(_register_schema(foo_namespace, foo_src, new_schema).topic.name)
        assert set(consumer_source.get_topics()) == set(consumer_source_topics)

    def test_not_pick_up_new_topics_in_diff_source(
        self,
        consumer_source,
        consumer_source_topics,
        _register_schema,
    ):
        assert consumer_source.get_topics() == consumer_source_topics
        new_schema = {
            'type': 'record',
            'name': 'src_two',
            'namespace': 'namespace_two',
            'fields': [{'type': 'bytes', 'name': 'md5'}]
        }
        _register_schema('namespace_two', 'src_two', new_schema)
        assert consumer_source.get_topics() == consumer_source_topics

    def test_bad_consuemr_source(self, bad_consumer_source):
        assert bad_consumer_source.get_topics() == []
        # calling `get_topic` multiple times will return the same empty list
        assert bad_consumer_source.get_topics() == []


class TestTopicsInFixedNamespaces(DynamicTopicSrcTests, NamespaceSrcSetupMixin):

    @pytest.fixture
    def consumer_source_cls(self):
        return TopicsInFixedNamespaces

    def test_empty_namespace(self):
        with pytest.raises(ValueError):
            TopicsInFixedNamespaces(namespace_names=[])

    def test_invalid_namespace(self):
        with pytest.raises(ValueError):
            TopicsInFixedNamespaces(namespace_names='')


class TestTopicsInSource(DynamicTopicSrcTests, SourceSrcSetupMixin):

    @pytest.fixture
    def consumer_source_cls(self):
        return TopicInSource

    def test_invalid_namespace(self, foo_src):
        with pytest.raises(ValueError):
            TopicInSource(namespace_name='', source_name=foo_src)

    def test_invalid_source(self, foo_namespace):
        with pytest.raises(ValueError):
            TopicInSource(namespace_name=foo_namespace, source_name='')


class TestTopicsInDataTarget(DynamicTopicSrcTests, DataTargetSetupMixin):

    @pytest.fixture
    def consumer_source_cls(self):
        return TopicInDataTarget

    def test_invalid_data_target(self):
        with pytest.raises(ValueError):
            TopicInDataTarget(data_target_id=0)


@pytest.mark.usefixtures('foo_schema')
class NewTopicOnlySrcTests(ConsumerSourceTestBase):

    def test_get_topics_first_time(self, consumer_source, foo_topic):
        assert consumer_source.get_topics() == [foo_topic]

    def test_get_topics_multiple_times_with_no_new_topics(
        self,
        consumer_source,
        foo_topic,
    ):
        # Wait some time so that the 1st query time is at least 1 second apart
        # from `foo_topic` creation timestamp to avoid flaky test
        time.sleep(1)
        assert consumer_source.get_topics() == [foo_topic]
        # Because the timestamp is rounded up to seconds, here it makes the 2nd
        # query time is at least 1 second after the 1st query to avoid flaky test
        time.sleep(1)
        assert consumer_source.get_topics() == []

    def test_pick_up_new_topics(
        self,
        consumer_source,
        foo_topic,
        foo_src,
        foo_namespace,
        _register_schema,
    ):
        time.sleep(1)
        assert consumer_source.get_topics() == [foo_topic]

        time.sleep(1)
        new_schema = {
            'type': 'record',
            'name': foo_src,
            'namespace': foo_namespace,
            'fields': [{'type': 'bytes', 'name': 'md5'}]
        }
        new_topic = _register_schema(foo_namespace, foo_src, new_schema).topic.name
        assert consumer_source.get_topics() == [new_topic]

    def test_not_pick_up_new_topics_in_diff_source(
        self,
        consumer_source,
        foo_topic,
        _register_schema,
    ):
        time.sleep(1)
        assert consumer_source.get_topics() == [foo_topic]

        time.sleep(1)
        new_schema = {
            'type': 'record',
            'name': 'src_two',
            'namespace': 'namespace_two',
            'fields': [{'type': 'bytes', 'name': 'md5'}]
        }
        _register_schema('namespace_two', 'src_two', new_schema)
        assert consumer_source.get_topics() == []

    def test_bad_consuemr_source(self, bad_consumer_source):
        assert bad_consumer_source.get_topics() == []
        # calling `get_topic` multiple times will return the same empty list
        assert bad_consumer_source.get_topics() == []


class TestNewTopicsOnlyInFixedNamespaces(NewTopicOnlySrcTests, NamespaceSrcSetupMixin):

    @pytest.fixture
    def consumer_source_cls(self):
        return NewTopicsOnlyInFixedNamespaces

    def test_invalid_namespace(self):
        with pytest.raises(ValueError):
            NewTopicsOnlyInFixedNamespaces(namespace_names=[])


class TestNewTopicOnlyInSource(NewTopicOnlySrcTests, SourceSrcSetupMixin):

    @pytest.fixture
    def consumer_source_cls(self):
        return NewTopicOnlyInSource

    def test_invalid_namespace(self, foo_src):
        with pytest.raises(ValueError):
            NewTopicOnlyInSource(namespace_name='', source_name=foo_src)

    def test_invalid_source(self, foo_namespace):
        with pytest.raises(ValueError):
            NewTopicOnlyInSource(namespace_name=foo_namespace, source_name='')


class TestNewTopicOnlyInDataTarget(NewTopicOnlySrcTests, DataTargetSetupMixin):

    @pytest.fixture
    def consumer_source_cls(self):
        return NewTopicOnlyInDataTarget

    def test_invalid_data_target(self):
        with pytest.raises(ValueError):
            NewTopicOnlyInDataTarget(data_target_id=0)
