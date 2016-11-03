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

import time
from datetime import datetime

from cached_property import cached_property

from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


class ConsumerSource(object):
    """Base class to specify the Kafka topics where the consumer would like to
    fetch messages from.
    """

    def get_topics(self):
        """Get a list of topics where the consumer get the messages from.  The
        derived class must implement this function.  It should return a list
        of topic names.

        Returns:
            List[str]: A list of topic names.  If no topics, an empty list is
            returned.
        """
        raise NotImplemented()

    @cached_property
    def schematizer(self):
        return get_schematizer()


class FixedTopics(ConsumerSource):
    """Consumer tails one or a fixed set of topics.

    Args:
        topic_names: Variable number of Kafka topic names to consume from.
            At least one topic must be specified.
    """

    def __init__(self, *topic_names):
        if not any(topic_names):
            raise ValueError("At least one topic must be specified.")
        self.topics = topic_names

    def get_topics(self):
        return self.topics


class TopicsInFixedNamespaces(ConsumerSource):
    """Consumer tails all the topics in specified namespaces.

    Args:
        namespace_names (tuple(str)): Variable number of namespace names in which all the
        topics will be tailed by the consumer.
    """

    def __init__(self, *namespace_names):
        if not any(namespace_names):
            raise ValueError("At least one namespace must be specified.")
        self.namespace_names = namespace_names

    def get_topics(self):
        topic_names = []
        for namespace_name in self.namespace_names:
            topic_names.extend(
                topic.name
                for topic in self.schematizer.get_topics_by_criteria(
                    namespace_name=namespace_name
                )
            )
        return topic_names


class TopicInSource(ConsumerSource):
    """Consumer tails the topics of specified source.  The source is specified
    by giving the namespace name and the source name stored in the Schematizer.
    For example, to tail all the topics associated to yelp-main biz table, the
    namespace name could be something like 'yelp` and the source name could be
    'biz'.

    Args:
        namespace_name (str): Namespace name of the specified source in which
            all the topics will be tailed by the consumer.
        source_name (str): Source name of the specified source in which all the
            topics will be tailed by the consumer.
    """

    def __init__(self, namespace_name, source_name):
        if not namespace_name:
            raise ValueError("namesapce_name must be specified.")
        if not source_name:
            raise ValueError("source_name must be specified.")
        self.namespace_name = namespace_name
        self.source_name = source_name

    def get_topics(self):
        topics = self.schematizer.get_topics_by_criteria(
            namespace_name=self.namespace_name,
            source_name=self.source_name
        )
        return [topic.name for topic in topics]


class FixedSchemas(ConsumerSource):
    """Consumer tails the topics given a list of schema ids. FixedSchema
    consumer source has `schema_to_topic_map` map that provides mapping
    from schema_id to its corresponding topic name.

    Args:
        schema_ids (tuple(int)): Variable number of schema IDs of avro schemas
        registered in the Schematizer.
    """

    def __init__(self, *schema_ids):
        if not any(schema_ids):
            raise ValueError("At least one schema id must be specified.")
        self.schema_ids = schema_ids

    def get_topics(self):
        topics = {
            self.schematizer.get_schema_by_id(schema_id).topic.name
            for schema_id in self.schema_ids
        }
        return list(topics)

    def get_schema_to_topic_map(self):
        schema_to_topic_map = {
            schema_id: self.schematizer.get_schema_by_id(schema_id).topic.name
            for schema_id in self.schema_ids
        }
        return schema_to_topic_map


class TopicInDataTarget(ConsumerSource):
    """Consumer tails the topics in the specified data target.

    Args:
        data_target_id (int): the ID of the data target stored in the Schematizer.
    """

    def __init__(self, data_target_id):
        if not data_target_id:
            raise ValueError("data_target_id must be specified.")
        self.data_target_id = data_target_id

    def get_topics(self):
        topics = self.schematizer.get_topics_by_data_target_id(self.data_target_id)
        return [topic.name for topic in topics]


class NewTopicsOnlyInFixedNamespaces(TopicsInFixedNamespaces):
    """Consumer tails all the topics in specified namespaces, but
    it internally keeps track of the previous query timestamp for each
    namespace and only returns the topics created after the last query
    timestamp, including the topics created at the last query timestamp for
    the namespace. It means the same topics returned previously may be included
    again if their created_at timestamp is right at previous query timestamp.

    Args:
        namespace_names (tuple(str)): Variable number of namespace names in which all the
        topics will be tailed by the consumer.
    """

    def __init__(self, *namespace_names):
        super(NewTopicsOnlyInFixedNamespaces, self).__init__(*namespace_names)
        self.last_query_timestamp = {}

    def get_topics(self):
        topic_names = []
        for namespace_name in self.namespace_names:
            created_after_timestamp = long(time.time())
            topic_names.extend(
                topic.name
                for topic in self.schematizer.get_topics_by_criteria(
                    namespace_name=namespace_name,
                    created_after=self.last_query_timestamp.get(namespace_name)
                )
            )
            self.last_query_timestamp[namespace_name] = created_after_timestamp
        return topic_names


class NewTopicOnlyInSource(TopicInSource):
    """Consumer tails the topics of the specified source, but it internally
    keeps track of the previous query timestamp and only returns the topics
    created after the last query timestamp, including the topics created at the
    last query timestamp.  It means the same topics returned previously may be
    included again if their created_at timestamp is right at previous query
    timestamp.

    Args:
        namespace_name (str): Namespace name of the specified source in which
            all the topics will be tailed by the consumer.
        source_name (str): Source name of the specified source in which all the
            topics will be tailed by the consumer.
    """

    def __init__(self, namespace_name, source_name):
        super(NewTopicOnlyInSource, self).__init__(
            namespace_name,
            source_name
        )
        self.last_query_timestamp = None

    def get_topics(self):
        topics = self.schematizer.get_topics_by_criteria(
            namespace_name=self.namespace_name,
            source_name=self.source_name,
            created_after=self.last_query_timestamp
        )
        self.last_query_timestamp = long(time.time())
        return [topic.name for topic in topics]


class NewTopicOnlyInDataTarget(TopicInDataTarget):
    """Consumer tails the topics in the specified data target, but it internally
    keeps track the previous query timestamp and only returns the topics created
    after the last query timestamp, including the topics created at the last
    query timestamp.  It means the same topics returned previously may be included
    again if their created_at timestamp is right at previous query timestamp.

    Args:
        data_target_id (int): the ID of the data target stored in the Schematizer.
    """

    def __init__(self, data_target_id):
        super(NewTopicOnlyInDataTarget, self).__init__(data_target_id)
        self.last_query_timestamp = None

    def _is_topic_created_after_last_query(self, topic):
        return (
            topic.created_at >= datetime.fromtimestamp(
                self.last_query_timestamp,
                topic.created_at.tzinfo
            )
        )

    def get_topics(self):
        topics = self.schematizer.get_topics_by_data_target_id(
            self.data_target_id
        )
        topic_names = [
            topic.name for topic in topics
            if (not self.last_query_timestamp or
                self._is_topic_created_after_last_query(topic)
                )
        ]
        self.last_query_timestamp = long(time.time())
        return topic_names
