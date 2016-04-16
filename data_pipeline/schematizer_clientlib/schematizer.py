# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson
from swaggerpy.exception import HTTPError

from data_pipeline.config import get_config
from data_pipeline.helpers.singleton import Singleton
from data_pipeline.schematizer_clientlib.models.avro_schema import _AvroSchema
from data_pipeline.schematizer_clientlib.models.consumer_group import _ConsumerGroup
from data_pipeline.schematizer_clientlib.models.consumer_group_data_source \
    import _ConsumerGroupDataSource
from data_pipeline.schematizer_clientlib.models.data_target import _DataTarget
from data_pipeline.schematizer_clientlib.models.refresh import _Refresh
from data_pipeline.schematizer_clientlib.models.schema_migration import _SchemaMigration
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

    __metaclass__ = Singleton

    def __init__(self):
        self._client = get_config().schematizer_client  # swaggerpy client
        self._cache = _Cache()
        self._avro_schema_cache = {}

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

    def _make_avro_schema_key(self, schema_json):
        return simplejson.dumps(schema_json, sort_keys=True)

    def get_schema_by_schema_json(self, schema_json):
        """ Get schema object if one exists for a given avro schema.
        If not, return None.

        Args:
            schema_json (dict or list): Python object representation of the
                avro schema json.

        Returns:
            (data_pipeline.schematizer_clientlib.models.avro_schema.AvroSchema):
                Avro Schema object.
        """
        cached_schema = self._avro_schema_cache.get(
            self._make_avro_schema_key(schema_json)
        )
        if cached_schema:
            _schema = _AvroSchema.from_cache_value(cached_schema)
            _schema.topic = self._get_topic_by_name(cached_schema['topic_name'])
            return _schema.to_result()
        else:
            # TODO(DATAPIPE-608|askatti): Add schematizer endpoint to return
            # Schema object given a schema_json
            return None

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

    def get_sources_by_namespace(self, namespace_name):
        """Get the list of sources in the specified namespace.

        Args:
            namespace_name (str): namespace name to look up

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.source.Source]):
                The list of schema sources in the given namespace.
        """
        response = self._call_api(
            api=self._client.namespaces.list_sources_by_namespace,
            params={'namespace': namespace_name}
        )
        result = []
        for resp_item in response:
            _source = _Source.from_response(resp_item)
            result.append(_source.to_result())
            self._set_cache_by_source(_source)
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
            topic_name (str): The topic name of which

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

    def register_schema_from_schema_json(
        self,
        namespace,
        source,
        schema_json,
        source_owner_email,
        contains_pii,
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
        created_after=None
    ):
        """Get all the topics that match specified criteria.  If no criterion
        is specified, it returns all the topics.

        Args:
            namespace_name (Optional[str]): namespace the topics belong to
            source_name (Optional[str]): name of the source topics belong to
            created_after (Optional[int]): Epoch timestamp the topics should be
                created after.  The topics created at the same timestamp are
                also included.

        Returns:
            (List[data_pipeline.schematizer_clientlib.models.topic.Topic]):
                list of topics that match given criteria.
        """
        response = self._call_api(
            api=self._client.topics.get_topics_by_criteria,
            params={
                'namespace': namespace_name,
                'source': source_name,
                'created_after': created_after
            }
        )
        result = []
        for resp_item in response:
            _topic = _Topic.from_response(resp_item)
            result.append(_topic.to_result())
            self._set_cache_by_topic(_topic)
        return result

    def create_data_target(self, target_type, destination):
        """ Create and return newly created data target.

        Args:
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
                'target_type': target_type,
                'destination': destination
            }
        )
        _data_target = _DataTarget.from_response(response)
        self._set_cache_by_data_target(_data_target)
        return _data_target.to_result()

    def get_data_target_by_id(self, data_target_id):
        """Get the data target of specified id.

        Args:
            data_target_id (int): The id of requested data target.

        Returns:
            (data_pipeline.data_targettizer_clientlib.models.data_target.DataTarget):
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
        created_after=None
    ):
        """Get all the refreshes that match the specified criteria. If no
        criterion is specified, it returns all refreshes.

        Args:
            namespace_name (Optional[str]): namespace the topics belong to.
            status (Optional[RefreshStatus]): The status associated with the refresh.
            created_after (Optional[int]): Epoch timestamp the refreshes should
                be created after. The refreshes created at the same timestamp
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
                'created_after': created_after
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
            priority (Priority): The priority of the refresh
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
            'priority': priority.name
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
            (data_pipeline.schematizer_client.models.data_source_type_enum.DataSourceTypEnum):
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
            except HTTPError:
                # List of topics may include topics not in schematizer
                pass
        return pkey_topics

    def get_schema_migration(self, new_schema, target_schema_type, old_schema=None):
        response = self._call_api(
            api=self._client.schema_migrations.get_schema_migration,
            request_body={
                'new_schema': new_schema,
                'old_schema': old_schema,
                'target_schema_type': target_schema_type
            }
        )
        _schema_migration = _SchemaMigration.from_response(response)
        return _schema_migration.to_result()

    def _call_api(self, api, params=None, request_body=None):
        # TODO(DATAPIPE-207|joshszep): Include retry strategy support
        request_params = params or {}
        if request_body:
            request_params['body'] = request_body
        request = api(**request_params)
        response = request.result()
        return response

    def _get_cached_schema(self, schema_id):
        _schema = self._cache.get_value(_AvroSchema, schema_id)
        if _schema:
            _schema.topic = self._get_topic_by_name(_schema.topic.name)
        return _schema

    def _set_cache_by_schema(self, new_schema):
        self._cache.set_value(new_schema.schema_id, new_schema)
        self._set_cache_by_topic(new_schema.topic)

        self._avro_schema_cache[
            self._make_avro_schema_key(new_schema.schema_json)
        ] = new_schema.to_cache_value()

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
