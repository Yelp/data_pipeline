# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson
from bravado.exception import HTTPError
from requests.exceptions import RequestException
from swagger_zipkin.zipkin_decorator import ZipkinClientDecorator

from data_pipeline._retry_util import ExpBackoffPolicy
from data_pipeline._retry_util import retry_on_exception
from data_pipeline._retry_util import RetryPolicy
from data_pipeline.config import get_config
from data_pipeline.helpers.singleton import Singleton
from data_pipeline.schematizer_clientlib.models.avro_schema import _AvroSchema
from data_pipeline.schematizer_clientlib.models.avro_schema_element import (
    _AvroSchemaElement
)
from data_pipeline.schematizer_clientlib.models.consumer_group import (
    _ConsumerGroup
)
from data_pipeline.schematizer_clientlib.models.consumer_group_data_source import (
    _ConsumerGroupDataSource
)
from data_pipeline.schematizer_clientlib.models.data_target import _DataTarget
from data_pipeline.schematizer_clientlib.models.meta_attr_namespace_mapping import (
    _MetaAttributeNamespaceMapping
)
from data_pipeline.schematizer_clientlib.models.meta_attr_source_mapping import (
    _MetaAttributeSourceMapping
)
from data_pipeline.schematizer_clientlib.models.namespace import _Namespace
from data_pipeline.schematizer_clientlib.models.refresh import _Refresh
from data_pipeline.schematizer_clientlib.models.source import _Source
from data_pipeline.schematizer_clientlib.models.topic import _Topic


class _Cache(object):
    """Cache used by Schematizer client.  This cache stores the schematizer
    entities, such as avro schemas, topics, sources, etc.

    This cache is currently limited to used by SchematizerClient only.  The
    cached value is expected to have `to_cache_value` and `from_cache_value`
    functions.  This limitation could be relaxed later if necessary.
    """

    def __init__(self):
        self._cache = {}

    def get_value(self, entity_type, entity_key):
        cache_key = self._get_cache_key(entity_type.__name__, entity_key)
        cache_value = self._cache.get(cache_key)
        return entity_type.from_cache_value(cache_value) if cache_value else None

    def set_value(self, entity_key, new_value):
        value_type_name = new_value.__class__.__name__
        cache_key = self._get_cache_key(value_type_name, entity_key)
        self._cache[cache_key] = new_value.to_cache_value()

    def _get_cache_key(self, entity_type_name, entity_key):
        return entity_type_name, entity_key


class SchematizerClient(object):
    """A client that interacts with Schematizer APIs.  It has built-in caching
    feature which caches avro schemas, topics, and etc.  Right now the cache is
    only in memory (TODO(DATAPIPE-162|joshszep): Implement persistent caching).

    It caches schemas, topics, and sources separately instead of caching nested
    objects to avoid storing duplicate data repeatedly.

    Currently the client will throw the HTTPError returned from the Schematizer
    service if an error occurs.  It could be nice to return more straight forward
    errors instead of HTTPError.  The work is tracked in DATAPIPE-352.
    """

    # This class potentially could grow relatively huge.  There may be a need to
    # split them in some way later if it's too huge.  One idea is to split related
    # functions into its own mix-in class, optionally in its own file, and have main
    # `SchematizerClient` class inherit this mix-in class.  The benefit of this
    # approach is the size of each mix-in class is manageable and easier to read.
    # The down side is it becomes harder to know if a function has been defined
    # since they're now in multiple classes/files.

    # TODO[clin|DATAPIPE-1518] change the rest of the hidden variables to normal
    # variables.

    __metaclass__ = Singleton

    # Default page size used for the pagination when calling bulk apis such as
    # `get_topics_by_criteria`. If a bulk api needs pagination, this should be
    # the page size by default. If the api needs custom page size, we should
    # consider a good way to implement it because I'd like to avoid adding a
    # bunch of such constants which clutter the code.
    DEFAULT_PAGE_SIZE = 20

    def __init__(self):
        self._bravado_client = get_config().schematizer_client
        self._client = ZipkinClientDecorator(self._bravado_client)
        self._cache = _Cache()

    def get_schema_by_id(self, schema_id):
        """Get the avro schema of given schema id.

        Args:
            schema_id (int): The id of requested avro schema.

        Returns:
            (data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                The requested avro Schema.
        """
        return self._get_schema_by_id(schema_id).to_result()

    def _get_schema_by_id(self, schema_id):
        _schema = self._get_cached_schema(schema_id)
        if _schema:
            return _schema

        response = self._call_api(
            api=self._client.schemas.get_schema_by_id,
            params={'schema_id': schema_id}
        )
        _schema = _AvroSchema.from_response(response)
        self._set_cache_by_schema(_schema)
        return _schema

    def get_schema_elements_by_schema_id(self, schema_id):
        """Get the avro schema elements of given schema id.

        Args:
            schema_id (int): The id of requested avro schema elements.

        Returns:
            (List of data_pipeline.schematizer_clientlib.models.avro_schema_element.AvroSchemaElement):
                The list requested avro Schema elements by schema_id.
        """
        # Filter out elements that represent the whole record (when element.element_name == None)
        return [
            element.to_result()
            for element in self._get_schema_elements_by_schema_id(schema_id)
            if element.element_name
        ]

    def _get_schema_elements_by_schema_id(self, schema_id):
        response = self._call_api(
            api=self._client.schemas.get_schema_elements_by_schema_id,
            params={'schema_id': schema_id}
        )
        _schema = _AvroSchemaElement.from_response(response)
        return _schema

    def get_schemas_created_after_date(
        self,
        created_after,
        min_id=0,
        page_size=10
    ):
        """ Get the avro schemas (excluding disabled schemas) created after the
        given datetime timestamp in ascending order of schema id. Limits the
        result to those with id greater than or equal to the min_id.

        Args:
            created_after (long): get schemas created at or after the given
                epoch timestamp.
            min_id (Optional[int]): Limits the result to those schemas with an
                id greater than or equal to given min_id (default: 0)
            page_size (Optional[int]): Limits the number of api calls and
                number of schemas to retrieve per call to avoid timeouts
                (default: 10).

        Returns:
            (List of data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                The list of avro schemas created after (inclusive) specified date.
        """
        return self._get_schemas_created_after_date(
            created_after,
            min_id,
            page_size
        )

    def get_schemas_by_criteria(
        self,
        created_after=0,
        min_id=0,
        count=10
    ):
        """ Get the avro schemas with the specified criteria: created_after,
        min_id and count.

        Args:
            created_after (Optional[long]): get schemas created at or after the given
                epoch timestamp (default 0)
            min_id (Optional[int]): Limits the result to those schemas with an
                id greater than or equal to given min_id (default: 0)
            count (Optional[int]): the maximum number of schemas returned in this call
                (default: 10).

        Returns:
            (List of data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                list of avro_schemas satifying the criteria specified in parameters
        """
        results = []
        response = self._call_api(
            api=self._client.schemas.get_schemas_created_after,
            params={
                'created_after': created_after,
                'count': count,
                'min_id': min_id
            }
        )

        for resp_item in response:
            _schema = _AvroSchema.from_response(resp_item)
            results.append(_schema.to_result())
            self._set_cache_by_schema(_schema)
        return results

    def _get_schemas_created_after_date(self, created_after, min_id, page_size):
        last_page_size = page_size
        result = []
        # Exit the loop when the number of schemas returned are not equal to
        # page_size. Denoting there are no more schemas to fetch from the
        # Schematizer.
        while last_page_size == page_size:
            response = self._call_api(
                api=self._client.schemas.get_schemas_created_after,
                params={
                    'created_after': created_after,
                    'count': page_size,
                    'min_id': min_id
                }
            )

            for resp_item in response:
                _schema = _AvroSchema.from_response(resp_item)
                result.append(_schema.to_result())
                self._set_cache_by_schema(_schema)
                min_id = _schema.schema_id + 1
            last_page_size = len(response)
        return result

    def get_schemas_by_topic(self, topic_name):
        """Get the list of schemas in the specified topic.

        Args:
            topic_name (str): name of the topic to look up

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                List of schemas in the given topic
        """
        response = self._call_api(
            api=self._client.topics.list_schemas_by_topic_name,
            params={"topic_name": topic_name}
        )
        result = []
        for resp_item in response:
            _schema = _AvroSchema.from_response(resp_item)
            result.append(_schema.to_result())
            self._set_cache_by_schema(_schema)
        return result

    def get_topic_by_name(self, topic_name):
        """Get the topic of given topic name.

        Args:
            topic_name (str): The name of requested topic.

        Returns:
            (data_pipeline.schematizer_clientlib.models.topic.Topic):
                The requested topic.
        """
        return self._get_topic_by_name(topic_name).to_result()

    def _get_topic_by_name(self, topic_name):
        _topic = self._get_cached_topic(topic_name)
        if _topic:
            return _topic

        response = self._call_api(
            api=self._client.topics.get_topic_by_topic_name,
            params={'topic_name': topic_name}
        )
        _topic = _Topic.from_response(response)
        self._set_cache_by_topic(_topic)
        return _topic

    def get_source_by_id(self, source_id):
        """Get the schema source of given source id.

        Args:
            source_id (int): The id of the source.

        Returns:
            (data_pipeline.schematizer_clientlib.models.topic.Source):
                The requested schema source.
        """
        return self._get_source_by_id(source_id).to_result()

    def _get_source_by_id(self, source_id):
        _source = self._cache.get_value(_Source, source_id)
        if _source:
            return _source

        response = self._call_api(
            api=self._client.sources.get_source_by_id,
            params={'source_id': source_id}
        )
        _source = _Source.from_response(response)
        self._set_cache_by_source(_source)
        return _source

    def get_namespaces(self):
        """Get the list of namespaces registered in the schematizer

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.namespace.Namespace])
                The list of namespaces
        """
        response = self._call_api(
            api=self._client.namespaces.list_namespaces,
            params={}
        )
        result = []
        for resp_item in response:
            _namespace = _Namespace.from_response(resp_item)
            result.append(_namespace.to_result())
        return result

    def get_sources_by_namespace(
        self,
        namespace_name,
        min_id=0,
        page_size=10
    ):
        """Get the list of sources in the specified namespace.

        Args:
            namespace_name (str): namespace name to look up
            min_id (Optional[int]): the returned sources should have id greater than or equal to the min_id
            page_size (Optional[int]): the number of sources to return in one api call to prevent timeout

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.source.Source]):
                The list of schema sources in the given namespace.
        """
        last_page_size = page_size
        result = []
        while last_page_size == page_size:
            response = self._call_api(
                api=self._client.namespaces.list_sources_by_namespace,
                params={
                    'namespace': namespace_name,
                    'min_id': min_id,
                    'count': page_size
                }
            )
            for resp_item in response:
                _source = _Source.from_response(resp_item)
                result.append(_source.to_result())
                self._set_cache_by_source(_source)
                min_id = _source.source_id + 1
            last_page_size = len(response)
        return result

    def get_sources(
        self,
        min_id=0,
        page_size=10
    ):
        """Get the sources that match specified criteria (min_id and
        page_size).  If no criterion is specified, it returns all the sources.

        Args:
            min_id (Optional[int]): Limits results to those sources with an id
                greater than or equal to given min_id (default: 0)
            page_size (Optional[int]): Maximum number of sources to retrieve
                per page. (default: 10)

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.Source]):
                list of topics that match given criteria.
        """
        last_page_size = page_size
        result = []
        while last_page_size == page_size:
            response = self._call_api(
                api=self._client.sources.list_sources,
                params={
                    'min_id': min_id,
                    'count': page_size
                }
            )
            for resp_item in response:
                _source = _Source.from_response(resp_item)
                result.append(_source.to_result())
                self._set_cache_by_source(_source)
                min_id = _source.source_id + 1
            last_page_size = len(response)
        return result

    def get_topics_by_source_id(self, source_id):
        """Get the list of topics of specified source id.

        Args:
            source_id (int): The id of the source to look up

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.topic.Topic]):
                The list of topics of given source.
        """
        response = self._call_api(
            api=self._client.sources.list_topics_by_source_id,
            params={'source_id': source_id}
        )
        result = []
        for resp_item in response:
            _topic = _Topic.from_response(resp_item)
            result.append(_topic.to_result())
            self._set_cache_by_topic(_topic)
        return result

    def get_latest_topic_by_source_id(self, source_id):
        """Get the lastest topic of specified source id

        Args:
            source_id (int): The id of the source to look up

        Returns:
            (data_pipeline.schematizer_clientlib.models.topic.Topic):
                The latest topic of given source
        """
        response = self._call_api(
            api=self._client.sources.get_latest_topic_by_source_id,
            params={'source_id': source_id}
        )
        _topic = _Topic.from_response(response)
        self._set_cache_by_topic(_topic)
        return _topic.to_result()

    def get_latest_schema_by_topic_name(self, topic_name):
        """Get the latest enabled schema of given topic.

        Args:
            topic_name (str): The name of the topic to look up

        Returns:
            (data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                The latest enabled avro schema of given topic.  It returns None
                if no such avro schema can be found.
        """
        response = self._call_api(
            api=self._client.topics.get_latest_schema_by_topic_name,
            params={'topic_name': topic_name}
        )
        _schema = _AvroSchema.from_response(response)
        self._set_cache_by_schema(_schema)
        return _schema.to_result()

    def register_schema(
        self,
        namespace,
        source,
        schema_str,
        source_owner_email,
        contains_pii,
        cluster_type='datapipe',
        base_schema_id=None
    ):
        """ Register a new schema and return newly created schema object.

        Args:
            namespace (str): The namespace the new schema is registered to.
            source (str): The source the new schema is registered to.
            schema_str (str): String representation of the avro schema.
            source_owner_email (str): The owner email of the given source.
            contains_pii (bool): Indicates if the schema being registered has
                at least one field that can potentially contain PII.
                See http://y/pii for help identifying what is or is not PII.
            cluster_type (Optional[str]): Kafka cluster type to connect to,
                like 'datapipe', 'scribe', etc. See http://y/datapipe_cluster_types
                for more info on cluster_types. Defaults to datapipe.
            base_schema_id (Optional[int]): The id of the original schema which
                the new schema was changed based on

        Returns:
            (data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                The newly created avro Schema.
        """
        request_body = {
            'schema': schema_str,
            'namespace': namespace,
            'source': source,
            'source_owner_email': source_owner_email,
            'contains_pii': contains_pii,
            'cluster_type': cluster_type
        }
        if base_schema_id:
            request_body['base_schema_id'] = base_schema_id
        response = self._call_api(
            api=self._client.schemas.register_schema,
            request_body=request_body
        )

        _schema = _AvroSchema.from_response(response)
        self._set_cache_by_schema(_schema)
        return _schema.to_result()

    def register_namespace_meta_attribute_mapping(
        self,
        namespace_name,
        meta_attr_schema_id
    ):
        """ Register a meta attribute for the given namespace.

        Args:
            namespace_name (str): The name of the namespace.
            meta_attr_schema_id (int): ID of the meta attribute (avro_schema)

        Returns:
            (data_pipeline.schematizer_clientlib.models.
            meta_attr_namespace_mapping.MetaAttributeNamespaceMapping): The
            newly created mapping between specified namespace and
            schema ID.
        """
        response = self._call_api(
            api=self._client.namespaces.register_namespace_meta_attribute_mapping,
            params={'namespace': namespace_name},
            request_body={
                'meta_attribute_schema_id': meta_attr_schema_id
            }
        )
        _meta_attr_mapping = _MetaAttributeNamespaceMapping.from_response(
            namespace_id=response.namespace_id,
            meta_attribute_schema_id=response.meta_attribute_schema_id
        )
        return _meta_attr_mapping.to_result()

    def delete_namespace_meta_attribute_mapping(
        self,
        namespace_name,
        meta_attr_schema_id
    ):
        """ Deletes a meta attribute from the given namespace.

        Args:
            namespace_name (str): The name of the namespace.
            meta_attr_schema_id (int): ID of the meta attribute (avro_schema)

        Returns:
            (data_pipeline.schematizer_clientlib.models.
            meta_attr_namespace_mapping.MetaAttributeNamespaceMapping): A
            successfully deleted mapping between specified namespace and
            schema ID.
        """
        response = self._call_api(
            api=self._client.namespaces.delete_namespace_meta_attribute_mapping,
            params={'namespace': namespace_name},
            request_body={
                'meta_attribute_schema_id': meta_attr_schema_id
            }
        )
        _meta_attr_mapping = _MetaAttributeNamespaceMapping.from_response(
            namespace_id=response.namespace_id,
            meta_attribute_schema_id=response.meta_attribute_schema_id
        )
        return _meta_attr_mapping.to_result()

    def get_namespace_meta_attribute_mappings(self, namespace_name):
        """ Returns meta attributes for the given namespace.

        Args:
            namespace_name (str): The name of the namespace.

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.
            meta_attr_namespace_mapping.MetaAttributeNamespaceMapping]): A
            list of mappings between specified namespace and schema IDs.
        """
        response = self._call_api(
            api=self._client.namespaces.get_namespace_meta_attribute_mappings,
            params={'namespace': namespace_name}
        )
        return [
            _MetaAttributeNamespaceMapping.from_response(
                namespace_id=resp_item.namespace_id,
                meta_attribute_schema_id=resp_item.meta_attribute_schema_id
            ).to_result()
            for resp_item in response]

    def register_source_meta_attribute_mapping(
        self,
        source_id,
        meta_attr_schema_id
    ):
        """ Register a meta attribute for the given source.

        Args:
            source_id (int): The ID of the source.
            meta_attr_schema_id (int): ID of the meta attribute (avro_schema)

        Returns:
            (data_pipeline.schematizer_clientlib.models.
            meta_attr_source_mapping.MetaAttributeSourceMapping): The
            newly created mapping between specified source and
            schema ID.
        """
        response = self._call_api(
            api=self._client.sources.register_source_meta_attribute_mapping,
            params={'source_id': source_id},
            request_body={
                'meta_attribute_schema_id': meta_attr_schema_id
            }
        )
        _meta_attr_mapping = _MetaAttributeSourceMapping.from_response(
            source_id=response.source_id,
            meta_attribute_schema_id=response.meta_attribute_schema_id
        )
        return _meta_attr_mapping.to_result()

    def delete_source_meta_attribute_mapping(
        self,
        source_id,
        meta_attr_schema_id
    ):
        """ Deletes a meta attribute from the given source.

        Args:
            source_id (int): The ID of the source.
            meta_attr_schema_id (int): ID of the meta attribute (avro_schema)

        Returns:
            (data_pipeline.schematizer_clientlib.models.
            meta_attr_source_mapping.MetaAttributeSourceMapping): A
            successfully deleted mapping between the specified source and
            schema.
        """
        response = self._call_api(
            api=self._client.sources.delete_source_meta_attribute_mapping,
            params={'source_id': source_id},
            request_body={
                'meta_attribute_schema_id': meta_attr_schema_id
            }
        )
        _meta_attr_mapping = _MetaAttributeSourceMapping.from_response(
            source_id=response.source_id,
            meta_attribute_schema_id=response.meta_attribute_schema_id
        )
        return _meta_attr_mapping.to_result()

    def get_source_meta_attribute_mappings(self, source_id):
        """ Returns meta attributes for the given source.

        Args:
            source_id (int): The ID of the source.

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.
            meta_attr_source_mapping.MetaAttributeSourceMapping]): A
            list of mappings between the specified source and schema IDs.
        """
        response = self._call_api(
            api=self._client.sources.get_source_meta_attribute_mappings,
            params={'source_id': source_id}
        )
        result = []
        for resp_item in response:
            _meta_attr_mapping = _MetaAttributeSourceMapping.from_response(
                source_id=resp_item.source_id,
                meta_attribute_schema_id=resp_item.meta_attribute_schema_id
            )
            result.append(_meta_attr_mapping.to_result())
        return result

    def get_meta_attributes_by_schema_id(self, schema_id):
        """ Returns meta attributes for the given schema.

        Args:
            schema_id (int): The ID of the Avro Schema.

        Returns:
            (List[int]): A list of meta attribute ids for the given Avro
            Schema.
        """
        return self._call_api(
            api=self._client.schemas.get_meta_attributes_by_schema_id,
            params={'schema_id': schema_id}
        )

    def register_schema_from_schema_json(
        self,
        namespace,
        source,
        schema_json,
        source_owner_email,
        contains_pii,
        cluster_type='datapipe',
        base_schema_id=None
    ):
        """ Register a new schema and return newly created schema object.

        Args:
            namespace (str): The namespace the new schema is registered to.
            source (str): The source the new schema is registered to.
            schema_json (dict or list): Python object representation of the
                avro schema json.
            source_owner_email (str): The owner email of the given source.
            contains_pii (bool): Indicates if the schema being registered has
                at least one field that can potentially contain PII.
                See http://y/pii for help identifying what is or is not PII.
            cluster_type (Optional[str]): Kafka cluster type to connect like
                datapipe, scribe, etc. See http://y/datapipe_cluster_types for
                more info on cluster_types. Defaults to datapipe.
            base_schema_id (Optional[int]): The id of the original schema which
                the new schema was changed based on

        Returns:
            (data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                The newly created avro Schema.
        """
        return self.register_schema(
            namespace=namespace,
            source=source,
            schema_str=simplejson.dumps(schema_json),
            source_owner_email=source_owner_email,
            contains_pii=contains_pii,
            cluster_type=cluster_type,
            base_schema_id=base_schema_id
        )

    def register_schema_from_mysql_stmts(
        self,
        namespace,
        source,
        source_owner_email,
        contains_pii,
        new_create_table_stmt,
        old_create_table_stmt=None,
        alter_table_stmt=None,
    ):
        """ Register schema based on mysql statements and return newly created
        schema.

        Args:
            namespace (str): The namespace the new schema is registered to.
            source (str): The source the new schema is registered to.
            source_owner_email (str): The owner email of the given source.
            contains_pii (bool): The flag indicating if schema contains pii.
            new_create_table_stmt (str): the mysql statement of creating new table.
            old_create_table_stmt (Optional[str]): the mysql statement of
                creating old table.
            alter_table_stmt (Optional[str]): the mysql statement of altering
                table schema.

        Returns:
            (data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                The newly created avro Schema.
        """
        request_body = {
            'namespace': namespace,
            'source': source,
            'new_create_table_stmt': new_create_table_stmt,
            'source_owner_email': source_owner_email,
            'contains_pii': contains_pii
        }
        if old_create_table_stmt:
            request_body['old_create_table_stmt'] = old_create_table_stmt
        if alter_table_stmt:
            request_body['alter_table_stmt'] = alter_table_stmt
        response = self._call_api(
            api=self._client.schemas.register_schema_from_mysql_stmts,
            request_body=request_body
        )

        _schema = _AvroSchema.from_response(response)
        self._set_cache_by_schema(_schema)
        return _schema.to_result()

    def get_topics_by_criteria(
        self,
        namespace_name=None,
        source_name=None,
        created_after=None,
        min_id=0,
        max_count=None
    ):
        """Get all the topics that match specified criteria.  If no criterion
        is specified, it returns all the topics.

        Args:
            namespace_name (Optional[str]): namespace the topics belong to
            source_name (Optional[str]): name of the source topics belong to
            created_after (Optional[int]): Epoch timestamp the topics should be
                created after.  The topics created at the same timestamp are
                also included.
            min_id (Optional[int]): Limits results to those topics with an id
                greater than or equal to given min_id (default: 0)
            max_count (Optional[int]): Maximum number of topics to retrieve. It
                must be a positive integer. If not specified, it returns all the
                topics.

        Returns:
            List[data_pipeline.schematizer_clientlib.models.topic.Topic]:
                list of topics that match given criteria. The returned topics
                are ordered by their topic id.

        Remarks:
            The function internally paginates through the topics if the max_count
            is too large to avoid timeout from the service.  The page size is set
            to 20 right now.
        """
        # The reason to set fixed page size internally is to make the pagination
        # transparent to the users. We can set a reasonable page size that makes
        # reasonable number of calls without causing timeout. This interface
        # should be sufficient for most of our use cases, so we probably don't
        # need to provide the interface for users to set the page size right now.
        max_count_specified = max_count is not None and max_count > 0
        page_size = self.DEFAULT_PAGE_SIZE
        should_get_more_topics = True
        result = []
        while should_get_more_topics:
            response = self._call_api(
                api=self._client.topics.get_topics_by_criteria,
                params={
                    'namespace': namespace_name,
                    'source': source_name,
                    'created_after': created_after,
                    'min_id': min_id,
                    'count': page_size
                }
            )
            topic = None
            for resp_item in response:
                topic = _Topic.from_response(resp_item)
                result.append(topic.to_result())
                self._set_cache_by_topic(topic)
                if max_count_specified and len(result) == max_count:
                    should_get_more_topics = False
                    break
            if topic:
                min_id = topic.topic_id + 1
            should_get_more_topics = (
                should_get_more_topics and len(response) >= page_size
            )
        return result

    def create_data_target(self, name, target_type, destination):
        """ Create and return newly created data target.

        Args:
            name (str): Name to uniquely identify the data target.
            target_type (str): The type of the data target, such as Redshift.
            destination (str): The actual location of the data target, such as
                Url of the Redshift cluster.

        Returns:
            (data_pipeline.schematizer_clientlib.models.data_target.DataTarget):
                The newly created data target.
        """
        response = self._call_api(
            api=self._client.data_targets.create_data_target,
            request_body={
                'name': name,
                'target_type': target_type,
                'destination': destination
            }
        )
        _data_target = _DataTarget.from_response(response)
        self._set_cache_by_data_target(_data_target)
        return _data_target.to_result()

    def get_data_targets_by_schema_id(self, schema_id):
        """Get data targets of specified schema id

        Args:
            schema_id (int): the id of a schema

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.data_target.DataTarget]):
                The list of data targets corresponding to the schema_id
        """
        return self._get_data_targets_by_schema_id(schema_id)

    def _get_data_targets_by_schema_id(self, schema_id):
        response = self._call_api(
            api=self._client.schemas.get_data_targets_by_schema_id,
            params={'schema_id': schema_id}
        )
        results = []
        for resp_item in response:
            _data_target = _DataTarget.from_response(resp_item)
            results.append(_data_target)
            self._set_cache_by_data_target(_data_target)
        return results

    def get_data_target_by_id(self, data_target_id):
        """Get the data target of specified id.

        Args:
            data_target_id (int): The id of requested data target.

        Returns:
            (data_pipeline.schematizer_clientlib.models.data_target.DataTarget):
                The requested data target.
        """
        return self._get_data_target_by_id(data_target_id).to_result()

    def _get_data_target_by_id(self, data_target_id):
        _data_target = self._cache.get_value(_DataTarget, data_target_id)
        if _data_target:
            return _data_target

        response = self._call_api(
            api=self._client.data_targets.get_data_target_by_id,
            params={'data_target_id': data_target_id}
        )
        _data_target = _DataTarget.from_response(response)
        self._set_cache_by_data_target(_data_target)
        return _data_target

    def get_data_target_by_name(self, data_target_name):
        """Get the data target of specified name.

        Args:
            data_target_name (str): The name of requested data target.

        Returns:
            (data_pipeline.schematizer_clientlib.models.data_target.DataTarget):
                The requested data target.
        """
        return self._get_data_target_by_name(data_target_name).to_result()

    def _get_data_target_by_name(self, data_target_name):
        _data_target = self._cache.get_value(_DataTarget, data_target_name)
        if _data_target:
            return _data_target

        response = self._call_api(
            api=self._client.data_targets.get_data_target_by_name,
            params={'data_target_name': data_target_name}
        )
        _data_target = _DataTarget.from_response(response)
        self._set_cache_by_data_target_name(_data_target)
        return _data_target

    def get_topics_by_data_target_id(self, data_target_id):
        """Get the list of topics associated to the specified data target id.

        Args:
            data_target_id (int): The id of the data target to look up

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.topic.Topic]):
                The list of topics of given data target.
        """
        response = self._call_api(
            api=self._client.data_targets.get_topics_by_data_target_id,
            params={'data_target_id': data_target_id}
        )
        result = []
        for resp_item in response:
            _topic = _Topic.from_response(resp_item)
            result.append(_topic.to_result())
            self._set_cache_by_topic(_topic)
        return result

    def get_refreshes_by_criteria(
        self,
        namespace_name=None,
        status=None,
        created_after=None,
        updated_after=None
    ):
        """Get all the refreshes that match the specified criteria. If no
        criterion is specified, it returns all refreshes.

        Args:
            namespace_name (Optional[str]): namespace the topics belong to.
            status (Optional[RefreshStatus]): The status associated with the refresh.
            created_after (Optional[int]): Epoch timestamp the refreshes should
                be created after. The refreshes created at the same timestamp
                are also included.
            updated_after (Optional[int]): Epoch timestamp the refreshes should
                be updated after. The refreshes updated at the same timestamp
                are also included.

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.refresh.Refresh]):
                list of refreshes that match the given criteria.
        """
        response = self._call_api(
            api=self._client.refreshes.get_refreshes_by_criteria,
            params={
                'namespace': namespace_name,
                'status': status.value if status is not None else None,
                'created_after': created_after,
                'updated_after': updated_after
            }
        )
        return [self._get_refresh_result_from_response(resp) for resp in response]

    def create_refresh(
        self,
        source_id,
        offset,
        batch_size,
        priority,
        filter_condition=None,
        avg_rows_per_second_cap=None
    ):
        """Register a refresh and returns the newly created refresh object.

        Args:
            source_id (int): The id of the source of the refresh.
            offset (int): The last known offset that has been refreshed.
            batch_size (int): The number of rows to be refreshed per batch.
            priority (int): The priority of the refresh
            filter_condition (Optional[str]): The filter condition associated with
             the refresh.
            avg_rows_per_second_cap (Optional[int]): Throughput throttle of the refresh.

        Returns:
            (data_pipeline.schematizer_clientlib.models.refresh.Refresh):
                The newly created Refresh.
        """
        request_body = {
            'offset': offset,
            'batch_size': batch_size,
            'priority': priority
        }
        if filter_condition:
            request_body['filter_condition'] = filter_condition
        if avg_rows_per_second_cap is not None:
            request_body['avg_rows_per_second_cap'] = avg_rows_per_second_cap
        response = self._call_api(
            api=self._client.sources.create_refresh,
            params={'source_id': source_id},
            request_body=request_body
        )
        return self._get_refresh_result_from_response(response)

    def update_refresh(self, refresh_id, status, offset):
        """Update the status of the refresh with specified refresh_id.

        Args:
            refresh_id (int): The id of the refresh being updated.
            status (RefreshStatus): New status of the refresh.
            offset (int): Last known offset that has been refreshed.

        Returns:
            (data_pipeline.schematizer_clientlib.models.refresh.Refresh):
                The updated Refresh.
        """
        request_body = {
            'status': status.value,
            'offset': offset
        }
        response = self._call_api(
            api=self._client.refreshes.update_refresh,
            params={'refresh_id': refresh_id},
            request_body=request_body
        )
        return self._get_refresh_result_from_response(response)

    def get_refreshes_by_namespace(self, namespace_name):
        """"Get the list of refreshes in the specified namespace.

        Args:
            namespace_name (str): namespace name to look up.

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.refresh.Refresh]):
                The list of refreshes in the given namespace.
        """
        response = self._call_api(
            api=self._client.namespaces.list_refreshes_by_namespace,
            params={'namespace': namespace_name}
        )
        return [self._get_refresh_result_from_response(resp) for resp in response]

    def get_refresh_by_id(self, refresh_id):
        """Get the refresh associated with specified refresh_id.

        Args:
            refresh_id (int): The id of the refresh.

        Returns:
            (data_pipeline.schematizer_clientlib.models.refresh.Refresh):
                The refresh with the given refresh_id.
        """
        response = self._call_api(
            api=self._client.refreshes.get_refresh_by_id,
            params={'refresh_id': str(refresh_id)}
        )
        return self._get_refresh_result_from_response(response)

    def create_consumer_group(self, group_name, data_target_id):
        """ Create and return newly created consumer group.

        Args:
            group_name (str): The name of the new consumer group.
            data_target_id (int): The id of the data target which the new consumer
                group associates to.

        Returns:
            (data_pipeline.schematizer_clientlib.models.consumer_group.ConsumerGroup):
                The newly created consumer group.
        """
        response = self._call_api(
            api=self._client.data_targets.create_consumer_group,
            params={'data_target_id': data_target_id},
            request_body={'group_name': group_name}
        )
        _consumer_group = _ConsumerGroup.from_response(response)
        self._set_cache_by_consumer_group(_consumer_group)
        return _consumer_group.to_result()

    def get_consumer_group_by_id(self, consumer_group_id):
        """Get the consumer group of specified id.

        Args:
            consumer_group_id (int): The id of requested consumer group.

        Returns:
            (data_pipeline.consumer_grouptizer_clientlib.models.consumer_group.ConsumerGroup):
                The requested consumer group.
        """
        return self._get_consumer_group_by_id(consumer_group_id).to_result()

    def _get_consumer_group_by_id(self, consumer_group_id):
        _consumer_group = self._get_cached_consumer_group(consumer_group_id)
        if _consumer_group:
            return _consumer_group

        response = self._call_api(
            api=self._client.consumer_groups.get_consumer_group_by_id,
            params={'consumer_group_id': consumer_group_id}
        )
        _consumer_group = _ConsumerGroup.from_response(response)
        self._set_cache_by_consumer_group(_consumer_group)
        return _consumer_group

    def create_consumer_group_data_source(
        self,
        consumer_group_id,
        data_source_type,
        data_source_id
    ):
        """ Create and return newly created mapping between specified data
        source and consumer group.

        Args:
            consumer_group_id (int): The id of the consumer group.
            data_source_type
            (data_pipeline.schematizer_client.models.data_source_type_enum.DataSourceTypeEnum):
                Type of the data source.
            data_source_id (int): The id of the data source, which could be a
            namespace id or source id.

        Returns:
            (data_pipeline.schematizer_clientlib.models.consumer_group_data_source
            .ConsumerGroupDataSource):
            The newly created mapping between specified consumer group and
            data source.
        """
        response = self._call_api(
            api=self._client.consumer_groups.create_consumer_group_data_source,
            params={'consumer_group_id': consumer_group_id},
            request_body={
                'data_source_type': data_source_type.name,
                'data_source_id': data_source_id
            }
        )
        _consumer_group_data_src = _ConsumerGroupDataSource.from_response(response)
        return _consumer_group_data_src.to_result()

    def is_avro_schema_compatible(
        self,
        avro_schema_str,
        source_name,
        namespace_name
    ):
        """Determines if given avro_schema is backward and forward compatible with all
        enabled schemas of given source (contained in given namespace).

        Note: Compatibility means the input schema should be able to deserialize data serialized
            by existing schemas within the same topic and vice versa.

        Args:
            avro_schema_str (str): json string representing avro_schema to check compatiblity on.
            source_name (str): name of the source that contains the schemas to check compatibiliy
                against.
            namespace_name (str): name of namespace containing the given source

        Returns:
            (boolean): If the given schema is compatible with all enabled schemas of the source."""
        response = self._call_api(
            api=self._client.compatibility.is_avro_schema_compatible,
            request_body={
                'source': source_name,
                'namespace': namespace_name,
                'schema': avro_schema_str
            }
        )
        return response

    def filter_topics_by_pkeys(self, topics):
        """ Create and return a new list of topic names built from topics,
        filtered by whether a topic's most recent schema has a primary_key

        Args:
            topics (list[str]): List of topic names

        Returns:
            Newly created list of topic names filtered by if the corresponding
            topics have primary_keys in their most recent schemas
        """
        pkey_topics = []
        for topic in topics:
            try:
                schema = self.get_latest_schema_by_topic_name(topic)
                if schema.primary_keys:
                    pkey_topics.append(topic)
            except HTTPError as error:
                # List of topics may include topics not in schematizer
                if error.response.status_code != 404:
                    raise
        return pkey_topics

    def get_schema_migration(self, new_schema, target_schema_type, old_schema=None):
        """ Get a list of of SQL statements needed to migrate to a desired avro schema
        from an old avro schema. In the absence of an old avro schema, the migration
        generates a plan to just create the new schema.

        Args:
            new_schema (dict): The avro schema to which we want to migrate
            target_schema_type
            (data_pipeline.schematizer_client.models.target_schema_type_enum.TargetSchemaTypeEnum):
                Type of the target schema.
            old_schema (Optional[dict]): The avro schema from which we want to migrate

        Returns:
            (data_pipeline.schematizer_clientlib.models.schema_migration
            .SchemaMigration):
            An object containing the pushplan required to migrate to the desired schema
        """
        request_body = {
            'new_schema': simplejson.dumps(new_schema),
            'target_schema_type': target_schema_type.name,
        }
        if old_schema:
            request_body['old_schema'] = simplejson.dumps(old_schema)

        response = self._call_api(
            api=self._client.schema_migrations.get_schema_migration,
            request_body=request_body
        )
        return response

    def _call_api(self, api, params=None, request_body=None):
        request_params = params or {}
        if request_body:
            request_params['body'] = request_body
        request = api(**request_params)
        retry_policy = RetryPolicy(
            ExpBackoffPolicy(with_jitter=True),
            max_retry_count=get_config().schematizer_client_max_connection_retry
        )
        response = retry_on_exception(
            retry_policy=retry_policy,
            retry_exceptions=RequestException,
            func_to_retry=self._get_api_result,
            request=request
        )
        return response

    def _get_api_result(self, request):
        return request.result()

    def _get_cached_schema(self, schema_id):
        _schema = self._cache.get_value(_AvroSchema, schema_id)
        if _schema:
            _schema.topic = self._get_topic_by_name(_schema.topic.name)
        return _schema

    def _set_cache_by_schema(self, new_schema):
        self._cache.set_value(new_schema.schema_id, new_schema)
        self._set_cache_by_topic(new_schema.topic)

    def _get_cached_topic(self, topic_name):
        _topic = self._cache.get_value(_Topic, topic_name)
        if _topic:
            _topic.source = self._get_source_by_id(_topic.source.source_id)
        return _topic

    def _set_cache_by_topic(self, new_topic):
        self._cache.set_value(new_topic.name, new_topic)
        self._set_cache_by_source(new_topic.source)

    def _set_cache_by_source(self, new_source):
        self._cache.set_value(new_source.source_id, new_source)

    def _set_cache_by_data_target(self, new_data_target):
        self._cache.set_value(new_data_target.data_target_id, new_data_target)

    def _set_cache_by_data_target_name(self, new_data_target):
        self._cache.set_value(new_data_target.name, new_data_target)

    def _get_cached_consumer_group(self, consumer_group_id):
        _consumer_group = self._cache.get_value(_ConsumerGroup, consumer_group_id)
        if _consumer_group:
            _consumer_group.data_target = self._get_data_target_by_id(
                _consumer_group.data_target.data_target_id
            )
        return _consumer_group

    def _set_cache_by_consumer_group(self, new_consumer_group):
        self._cache.set_value(
            new_consumer_group.consumer_group_id,
            new_consumer_group
        )
        self._set_cache_by_data_target(new_consumer_group.data_target)

    def _get_refresh_result_from_response(self, response):
        _refresh = _Refresh.from_response(response)
        return _refresh.to_result()


def get_schematizer():
    return SchematizerClient()
