# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from swaggerpy import client


class SchemaCache(object):

    """ A cache for mapping schema_id's to their schemas and to their
        transformed schema_ids
    """

    def __init__(self, api_client):
        if api_client:
            self._api_client = api_client
        else:
            self._api_client = client.get_client(
                # TODO: configurable schematizer swagger-py client URL
                "http://srv1-uswest1adevc:31024/api-docs"
            )
        self.schema_id_to_schema_map = {}
        self.schema_id_to_topic_map = {}
        self.base_to_transformed_schema_id_map = {}

    @property
    def api_client(self):
        return self._api_client

    def get_transformed_schema_id(self, schema_id):
        """ Get the cached transformed schema_id corresponding to the given
            base_schema_id if it is known, otherwise None is returned

        :param int schema_id: The base schema_id
        :return: cached transformed schema_id if known
        :rtype: int | None
        """
        return self.base_to_transformed_schema_id_map.get(schema_id, None)

    def register_transformed_schema(
            self, base_schema_id, namespace, source, schema, owner_email
    ):
        """ Register a new schema and return it's schema_id and topic

        :param int base_schema_id:
            the schema_id the new schema was transformed from
        :param str namespace:
            the namespace the new schema should be registered to
        :param str source: the source the new schema should be registered to
        :param str schema: the new schema as a json string
        :param str owner_email: the owner email for the new schema
        :return: The new schema_id and the new topic name
        :rtype: (int, string)
        """
        register_response = self.api_client.schemas.register_schema(
            base_schema_id=base_schema_id,
            schema=schema,
            namespace=namespace,
            source=source,
            source_owner_email=owner_email
        ).result()
        transformed_id = register_response.schema_id
        self.schema_id_to_schema_map[transformed_id] = register_response.schema
        self.base_to_transformed_schema_id_map[base_schema_id] = transformed_id
        self.schema_id_to_topic_map[transformed_id] = register_response.topic
        return register_response.schema_id, register_response.topic

    def get_topic_for_schema_id(self, schema_id):
        """ Get the topic name for a given schema_id

        :param int schema_id: the schema_id you are curious about
        :return: The topic name
        :rtype: str
        """
        if schema_id in self.schema_id_to_topic_map:
            return self.schema_id_to_topic_map[schema_id]
        else:
            schema_response = self.api_client.schemas.get_schema_by_id(
                schema_id=schema_id
            ).result()
            return schema_response.topic

    def get_schema(self, schema_id):
        """ Get the schema corresponding to the given schema_id, handling cache
            misses if required

            Note: This will be the point to integrate more sophisticated caching
            if required

        :param int schema_id: the schema_id to use for lookup
        :return: the schema as a json string
        :rtype: str
        """
        if schema_id in self.schema_id_to_schema_map:
            schema = self.schema_id_to_schema_map[schema_id]
        else:
            schema = self._retrieve_schema_from_schematizer(schema_id)
            self.schema_id_to_schema_map[schema_id] = schema
        return schema

    def _retrieve_schema_from_schematizer(self, schema_id):
        schema_response = self.api_client.schemas.get_schema_by_id(
            schema_id=schema_id
        ).result()
        return schema_response.schema
