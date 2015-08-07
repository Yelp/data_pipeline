# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline._avro_util import get_avro_schema_object
from data_pipeline.config import get_config


class SchemaCache(object):
    """ A cache for mapping schema_id's to their schemas and to their
        transformed schema_ids.

        Currently this only holds an in-memory cache.
        TODO(DATAPIPE-162|joshszep): Implement persistent caching
    """

    def __init__(self):
        self.schema_id_to_schema_map = {}
        self.schema_id_to_topic_map = {}
        self.base_to_transformed_schema_id_map = {}

    @property
    def schematizer_client(self):
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

    def register_transformed_schema(
        self,
        base_schema_id,
        namespace,
        source,
        schema,
        owner_email,
        contains_pii
    ):
        """ Register a new schema and return it's schema_id and topic

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
        return register_response.schema_id, new_topic_name

    def get_topic_for_schema_id(self, schema_id):
        """ Get the topic name for a given schema_id

        Args:
            schema_id (int): The schema_id you are curious about.

        Returns:
            (str): The topic name for the given schema_id
        """
        topic_name = self.schema_id_to_topic_map.get(
            schema_id,
            self._retrieve_topic_name_from_schematizer(schema_id)
        )
        self.schema_id_to_topic_map[schema_id] = topic_name
        return topic_name

    def get_schema(self, schema_id):
        """ Get the schema corresponding to the given schema_id, handling cache
            misses if required

        Args:
            schema_id (int): The schema_id to use for lookup.

        Returns:
            (avro.schema.Schema): The avro Schema object
        """
        schema = self.schema_id_to_schema_map.get(
            schema_id,
            self._retrieve_avro_schema_from_schematizer(schema_id)
        )
        self.schema_id_to_schema_map[schema_id] = schema
        return schema

    def _retrieve_schema_from_schematizer(self, schema_id):
        # TODO(DATAPIPE-207|joshszep): Include retry strategy support
        return self.schematizer_client.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()

    def _retrieve_topic_name_from_schematizer(self, schema_id):
        return self._retrieve_schema_from_schematizer(schema_id).topic.name

    def _retrieve_avro_schema_from_schematizer(self, schema_id):
        return get_avro_schema_object(
            self._retrieve_schema_from_schematizer(schema_id).schema
        )

_schema_cache = SchemaCache()


def get_schema_cache():
    return _schema_cache
