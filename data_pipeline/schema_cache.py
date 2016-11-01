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

import warnings
from collections import namedtuple

import simplejson
from data_pipeline_avro_util.util import get_avro_schema_object

from data_pipeline.config import get_config


# TODO([DATAPIPE-396|clin]): Revise these namedtuples to their own classes
# as soon as we can! They should be separate classes in their own files in
# separate folder along with this schematizer clientlib module.
SchemaInfo = namedtuple('SchemaInfo', [
    'schema_id',
    'topic_name'
])

AvroSchema = namedtuple(
    'AvroSchema',
    ['schema_id', 'schema', 'topic', 'base_schema_id', 'created_at']
)
Topic = namedtuple('Topic', ['topic_id', 'name', 'source', 'created_at'])
Source = namedtuple('Source', ['source_id', 'name', 'namespace', 'created_at'])
Namespace = namedtuple('Namespace', ['namespace_id', 'name', 'created_at'])


class SchematizerClient(object):
    """This old Schematizer cache is now deprecated.

    Client that provides high level api calls to interact with Schematizer
    service.  It has a built-in cache that maps schema_id's to their schemas
    and to their transformed schema_ids.

        Currently this only holds an in-memory cache.
        TODO(DATAPIPE-162|joshszep): Implement persistent caching
    """

    def __init__(self):
        warnings.simplefilter('always', category=DeprecationWarning)
        warnings.warn("data_pipeline.schema_cache.SchematizerClient is deprecated.",
                      DeprecationWarning)
        self.schema_id_to_schema_map = {}
        self.schema_id_to_topic_map = {}
        self.base_to_transformed_schema_id_map = {}
        self.schema_id_to_pii_map = {}

    @property
    def schematizer_client(self):
        """TODO[DATAPIPE-396|clin]: change this to be private once this class
        is converted to the true schematizer client.
        """
        return get_config().schematizer_client

    def get_transformed_schema_id(self, schema_id):
        """ Get the cached transformed schema_id corresponding to the given
            base_schema_id if it is known, otherwise None is returned

        Args:
            schema_id (int): The base schema_id

        Returns:
            int: cached transformed schema_id if known, None otherwise
        """
        return self.base_to_transformed_schema_id_map.get(schema_id, None)

    def _register_schema(
        self,
        namespace,
        source,
        schema_str,
        owner_email,
        contains_pii,
        base_schema_id=None
    ):
        """This is an initial step to rename the existing `register_transformed_schema`
        function. The function is to register an Avro schema, with optional base
        schema. So keep the name neutral and not AST specific. For new code, please
        use this function. The existing function `register_transformed_schema` will
        continue to work until we're safe to rename it.

        Right now the function is private and will stay private until it's ready.
        """
        request_body = {
            'schema': schema_str,
            'namespace': namespace,
            'source': source,
            'source_owner_email': owner_email,
            'contains_pii': contains_pii,
        }
        if base_schema_id:
            request_body['base_schema_id'] = base_schema_id
        schema_resp = self.schematizer_client.schemas.register_schema(
            body=request_body
        ).result()
        # TODO[DATAPIPE-396|clin]: add caching as part of DATAPIPE-396
        return self._construct_schema(schema_resp)

    def register_schema_by_schema_json(
        self,
        namespace,
        source,
        schema_json,
        owner_email,
        contains_pii,
        base_schema_id=None
    ):
        """Register a new avro schema.

        Args:
            namespace (str): The namespace the new schema belongs to
            source (str): The source the new schema belongs to.
            schema_json (dict or list): The Python object representation of the
                new avro schema json.
            owner_email (str): The email of the source owner.
            contains_pii (bool): Indicates whether the new schema contains
                field(s) that store pii information.  See http://y/pii for
                help identifying what is or is not PII.
            base_schema_id (Optional[int]): The schema_id of the original schema
                which the new schema is changed based on.

        Returns:
            `data_pipeline.schema_cache.AvroSchema`: The object containing the
                information of newly created avro schema.
        """
        return self._register_schema(
            namespace=namespace,
            source=source,
            schema_str=simplejson.dumps(schema_json),
            owner_email=owner_email,
            contains_pii=contains_pii,
            base_schema_id=base_schema_id
        )

    def register_transformed_schema(
        self,
        base_schema_id,
        namespace,
        source,
        schema,
        owner_email,
        contains_pii
    ):
        """ Register a new schema and return it's schema_id and topic.
        Note that this function is going to be deprecated soon.  Please use
        `register_schema` function instead for moving forward.

        Args:
            base_schema_id (int): The schema_id of the original schema from
                which the the new schema was transformed
            namespace (str): The namespace the new schema should be registered
                to.
            source (str): The source the new schema should be registered to.
            schema (str): The new schema in json string representation.
            owner_email (str): The owner email for the new schema.
            contains_pii (bool): Indicates that the schema being registered has
                at least one field that can potentially contain PII.
                See http://y/pii for help identifying what is or is not PII.

        Returns:
            (int, string): The new schema_id and the new topic name
        """
        request_body = {
            'base_schema_id': base_schema_id,
            'schema': schema,
            'namespace': namespace,
            'source': source,
            'source_owner_email': owner_email,
            'contains_pii': contains_pii,
        }
        register_response = self.schematizer_client.schemas.register_schema(
            body=request_body
        ).result()
        transformed_id = register_response.schema_id
        self.schema_id_to_schema_map[transformed_id] = register_response.schema
        self.base_to_transformed_schema_id_map[base_schema_id] = transformed_id
        new_topic_name = register_response.topic.name
        self.schema_id_to_topic_map[transformed_id] = new_topic_name
        return SchemaInfo(schema_id=register_response.schema_id, topic_name=new_topic_name)

    def register_schema_from_mysql_stmts(
            self,
            new_create_table_stmt,
            namespace,
            source,
            owner_email,
            contains_pii,
            old_create_table_stmt=None,
            alter_table_stmt=None,
    ):
        """ Register schema based on mysql statements and return it's schema_id
            and topic.

        Args:
            new_create_table_stmt (str): the mysql statement of creating new table.
            namespace (str): The namespace the new schema should be registered to.
            source (str): The source the new schema should be registered to.
            owner_email (str): The owner email for the new schema.
            contains_pii (bool): The flag indicating if schema contains pii.
            old_create_table_stmt (str optional): the mysql statement of creating old table.
            alter_table_stmt (str optional): the mysql statement of altering table schema.

        Returns:
            (int, string): The new schema_id and the new topic name
        """
        request_body = {
            'new_create_table_stmt': new_create_table_stmt,
            'namespace': namespace,
            'source': source,
            'source_owner_email': owner_email,
            'contains_pii': contains_pii
        }
        if old_create_table_stmt:
            request_body['old_create_table_stmt'] = old_create_table_stmt
        if alter_table_stmt:
            request_body['alter_table_stmt'] = alter_table_stmt
        register_response = self.schematizer_client.schemas.register_schema_from_mysql_stmts(
            body=request_body
        ).result()
        schema_id = register_response.schema_id
        self.schema_id_to_schema_map[schema_id] = register_response.schema
        new_topic_name = register_response.topic.name
        self.schema_id_to_topic_map[schema_id] = new_topic_name
        return SchemaInfo(schema_id=register_response.schema_id, topic_name=new_topic_name)

    def get_topic_for_schema_id(self, schema_id):
        """ Get the topic name for a given schema_id

        Args:
            schema_id (int): The schema_id you are curious about.

        Returns:
            (str): The topic name for the given schema_id
        """
        if schema_id not in self.schema_id_to_topic_map:
            self.schema_id_to_topic_map[schema_id] = self._retrieve_topic_name_from_schematizer(schema_id)
        return self.schema_id_to_topic_map[schema_id]

    def get_contains_pii_for_schema_id(self, schema_id):
        if schema_id not in self.schema_id_to_pii_map:
            self.schema_id_to_pii_map[schema_id] = self._retrieve_contains_pii_from_schematizer(schema_id)
        return self.schema_id_to_pii_map[schema_id]

    def get_schema(self, schema_id):
        """ Get the schema corresponding to the given schema_id, handling cache
            misses if required

        Args:
            schema_id (int): The schema_id to use for lookup.

        Returns:
            (avro.schema.Schema): The avro Schema object
        """
        if schema_id not in self.schema_id_to_schema_map:
            self.schema_id_to_schema_map[schema_id] = self._retrieve_avro_schema_from_schematizer(schema_id)
        return self.schema_id_to_schema_map[schema_id]

    def _get_schema_from_schematizer(self, schema_id):
        # TODO(DATAPIPE-207|joshszep): Include retry strategy support
        return self.schematizer_client.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()

    def _retrieve_topic_name_from_schematizer(self, schema_id):
        return self._get_schema_from_schematizer(schema_id).topic.name

    def _retrieve_contains_pii_from_schematizer(self, schema_id):
        return self._get_schema_from_schematizer(schema_id).topic.contains_pii

    def _retrieve_avro_schema_from_schematizer(self, schema_id):
        return get_avro_schema_object(
            self._get_schema_from_schematizer(schema_id).schema
        )

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
            [schema_cache.Topic]: list of topics that match given criteria.
        """
        topics_resp = self.schematizer_client.topics.get_topics_by_criteria(
            namespace=namespace_name,
            source=source_name,
            created_after=created_after
        ).result()
        return [self._construct_topic(t) for t in topics_resp]

    def _construct_schema(self, response):
        # TODO([DATAPIPE-396|clin]): This should be replaced with class constructor
        # once the AvroSchema namedtuple is replaced with the class.
        return AvroSchema(
            schema_id=response.schema_id,
            schema=response.schema,
            topic=self._construct_topic(response.topic),
            base_schema_id=response.base_schema_id,
            created_at=response.created_at
        )

    def _construct_topic(self, response):
        # TODO([DATAPIPE-396|clin]): This should be replaced with class constructor
        # once the Topic namedtuple is replaced with the class.
        return Topic(
            topic_id=response.topic_id,
            name=response.name,
            source=self._construct_source(response.source),
            created_at=response.created_at
        )

    def _construct_source(self, response):
        # TODO([DATAPIPE-396|clin]): This should be replaced with class constructor
        # once the Source namedtuple is replaced with the class.
        return Source(
            source_id=response.source_id,
            name=response.name,
            namespace=self._construct_namespace(response.namespace),
            created_at=response.created_at
        )

    def _construct_namespace(self, response):
        # TODO([DATAPIPE-396|clin]): This should be replaced with class constructor
        # once the Namespace namedtuple is replaced with the class.
        return Namespace(
            namespace_id=response.namespace_id,
            name=response.name,
            created_at=response.created_at
        )


_schematizer_client = SchematizerClient()


def get_schema_cache():
    return _schematizer_client


def get_schematizer_client():
    """This is an initial step for DATAPIPE-396 to eventually rename the
    schema cache to schematizer client.  For new code that needs to access
    schema_cache, use this function instead of `get_schema_cache` function.
    For the existing code, `get_schema_cache` will continue to work in the
    meantime.
    """
    return _schematizer_client
