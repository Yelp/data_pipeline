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

from collections import namedtuple


class PositionData(namedtuple("PositionData", [
    "last_published_message_position_info",
    "topic_to_last_position_info_map",
    "topic_to_kafka_offset_map",
    "merged_upstream_position_info_map"
])):
    """Contains information about the last messages successfully published into
    Kafka.

    **Examples**:

        When first activated, position data is minimal:

        >>> from data_pipeline.producer import Producer
        >>> with Producer() as producer:
        ...     position_data = producer.get_checkpoint_position_data()
        >>> position_data.last_published_message_position_info == None
        True
        >>> position_data.topic_to_last_position_info_map
        {}
        >>> position_data.topic_to_kafka_offset_map
        {}

        :class:`PositionData` will be updated as data is actually published:

        >>> from data_pipeline.message import CreateMessage
        >>> with Producer() as producer:
        ...     producer.publish(CreateMessage(
        ...         schema_id=10,
        ...         payload=bytes(10),
        ...         upstream_position_info={str('upstream_offset'): str('offset-info')}
        ...     ))
        ...     producer.flush()
        ...     position_data = producer.get_checkpoint_position_data()
        >>> position_data.last_published_message_position_info
        {'upstream_offset': 'offset-info'}
        >>> position_data.topic_to_last_position_info_map
        {'my-topic': {'upstream_offset': 'offset-info'}}
        >>> position_data.topic_to_kafka_offset_map   # doctest: +ELLIPSIS
        {'my-topic': ...}
        >>> position_data.merged_upstream_position_info_map
        {'upstream_offset': 'offset-info'}

    Warning:

        :class:`PositionData` returned by
        :meth:`data_pipeline.producer.Producer.get_checkpoint_position_data` is
        only the source of truth for topics that have messages published to
        them during the current session.  If this data is persisted, it should
        be merged with data from prior sessions, preferring data from the
        current session only when it is available.

    Attributes:
        last_published_message_position_info (None, dict): If a message has been
            published successfully, this will contain the
            :attr:`data_pipeline.message.Message.upstream_position_info` from
            the last message that was successfully published, irrespective of
            topic.  In this context, last refers to messages sorted by the
            order they were passed into :meth:`Producer.publish`.  This field
            is meant to be used if upstream data originates from a single
            source.

            **Example**:

                Assuming a dictionary containing gtid and offset information is
                passed into
                :attr:`data_pipeline.message.Message.upstream_position_info`::

                    {'gtid': 'UPSTREAM_GTID_INFO', 'offset': 'UPSTREAM_OFFSET_INFO'}

        topic_to_last_position_info_map (dict[str, dict]): Maps from topic to
            the last :attr:`data_pipeline.message.Message.upstream_position_info`
            that was successfully published for each topic, where last refers to
            messages sorted by the order they were passed into
            :meth:`Producer.publish`.  The dictionary starts empty, and only
            contains information about topics published into during this
            session.  This field is meant to be used if upstream data
            originates from multiple sources.

            **Example**:

                The dictionary will contain the last upstream_position_info
                for each topic.  Assuming a message had only been published to
                `my-topic`::

                    {'my-topic': {'gtid': 'UPSTREAM_GTID_INFO', 'offset': 'UPSTREAM_OFFSET_INFO'}}

        topic_to_kafka_offset_map (dict[str, int]): This maps from each kafka
            topic to the offset following the last published message, which will
            correspond to the offset of the next message published into the
            topic.

            **Example**:

                The dictionary will contain a Kafka offset for each topic
                publised to in this session::

                    {'topic1': offset1, 'topic2': offset2}

        merged_upstream_position_info_map (dict): This dictionary starts empty,
            and contains a deep merge of
            :attr:`data_pipeline.message.Message.upstream_position_info`
            for all messages that have been successfully published.  This
            structure is particularly useful to track the combined upstream
            position info for multiple topics.

            **Example**:

                This dictionary starts empty, and each messages
                `upstream_position_info` will be deep-merged into it::

                    {'topic1': {0: offset1}, 'topic2': {0, offset2}}

                When implementing a kafka-to-kafka transformer pipeline, this
                makes it easy to build up a structure to pass into
                :meth:`data_pipeline.base_consumer.BaseConsumer.commit_offsets`.
                Each transformed message would include `upstream_position_info`
                like::

                    {topic: {partition: offset}}
    """
    # This is a class instead of a namedtuple so the docstring can be
    # set.
