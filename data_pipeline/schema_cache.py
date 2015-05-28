# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class SchemaCache(object):
    """ A cache for mapping schema_id's to their schemas and to their
        transformed schema_ids
    """

    def __init__(self, schematizer_client):
        self.schematizer_client = schematizer_client
        self.schema_id_to_schema_map = {}
        self.schema_id_to_topic_map = {}
        self.base_to_transformed_schema_id_map = {}

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
            self, base_schema_id, namespace, source, schema, owner_email
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

        Returns:
            (int, string): The new schema_id and the new topic name
        """
        register_response = self.schematizer_client.schemas.register_schema(
            body={
                'base_schema_id': base_schema_id,
                'schema': schema,
                'namespace': namespace,
                'source': source,
                'source_owner_email': owner_email
            }
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
        if schema_id in self.schema_id_to_topic_map:
            return self.schema_id_to_topic_map[schema_id]
        else:
            schema_response = self.schematizer_client.schemas.get_schema_by_id(
                schema_id=schema_id
            ).result()
            return schema_response.topic

    def get_schema(self, schema_id):
        """ Get the schema corresponding to the given schema_id, handling cache
            misses if required

            Note: This will be the point to integrate more sophisticated caching
            if required

        Args:
            schema_id (int): The schema_id to use for lookup.

        Returns:
            (str): The schema as a json string
        """
        if schema_id in self.schema_id_to_schema_map:
            schema = self.schema_id_to_schema_map[schema_id]
        else:
            schema = self._retrieve_schema_from_schematizer(schema_id)
            self.schema_id_to_schema_map[schema_id] = schema
        return schema

    def _retrieve_schema_from_schematizer(self, schema_id):
        schema_response = self.schematizer_client.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()
        return schema_response.schema
