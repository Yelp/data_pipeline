# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple


class PositionData(namedtuple("PositionData", [
    "last_published_message_position_info",
    "topic_to_last_position_info_map",
    "topic_to_kafka_offset_map"
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

        >>> from data_pipeline.message import Message
        >>> from data_pipeline.message_type import MessageType
        >>> with Producer() as producer:
        ...     producer.publish(Message(
        ...         str('my-topic'),
        ...         10,
        ...         bytes(10),
        ...         MessageType.create,
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
    """
    # This is a class instead of a namedtuple so the docstring can be
    # overridden.
