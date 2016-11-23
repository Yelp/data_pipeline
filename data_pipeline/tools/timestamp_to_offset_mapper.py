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

from kafka.consumer import SimpleConsumer
from kafka_utils.util import offsets

from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope

logger = get_config().logger

# TODO (joshszep|DATAPIPE-2119) We could use some tests for this stuff.. :)


def get_first_offset_at_or_after_start_timestamp(
    kafka_client,
    topics,
    start_timestamp
):
    """Uses binary search to find the first offset that comes after
    start_timestamp for each topic in topics. If multiple items are present for
    a timestamp, the first one (closer to low_mark) is returned back.

    Outputs a result_topic_to_consumer_topic_state_map which can be used to
    set offsets

    :param kafka_client: kafka client to be used for getting watermarks and
        binary search.
    :param topics: a list of topics. eg. ['test_topic_1', 'test_topic_2']
    :param start_timestamp: epoch timestamp eg. 1463086536

    :returns: a dict mapping topic to the nearest starting timestamp.
              eg.
              {'test_topic_1': ConsumerTopicState({0: 43}, None),
              'test_topic_2': ConsumerTopicState({0: 55, 1: 32}, None)}
    """
    watermarks = offsets.get_topics_watermarks(
        kafka_client,
        topics,
        raise_on_error=False
    )

    topic_to_consumer_map = _build_topic_to_consumer_map(kafka_client, topics)

    topic_to_consumer_topic_state_map = _build_topic_to_consumer_topic_state_map(
        watermarks
    )
    topic_to_range_map = _build_topic_to_range_map(watermarks)
    result_topic_to_consumer_topic_state_map = _build_empty_topic_to_consumer_topic_state_map(
        topics
    )

    _move_finished_topics_to_result_map(
        topic_to_consumer_topic_state_map,
        topic_to_range_map,
        result_topic_to_consumer_topic_state_map
    )
    while topic_to_consumer_topic_state_map:
        _get_message_and_alter_range(
            topic_to_consumer_map,
            start_timestamp,
            topic_to_consumer_topic_state_map,
            topic_to_range_map,
            result_topic_to_consumer_topic_state_map
        )

    logger.info(
        "Got topic offsets based on start-date: {}".format(
            result_topic_to_consumer_topic_state_map
        )
    )
    return result_topic_to_consumer_topic_state_map


def _build_topic_to_consumer_map(kafka_client, topics):
    """Build a mapping of topic to SimpleConsumer object for each topic. We
    use a single SingleConsumer per topic since SimpleConsumer allows us to
    seek, but only supports a single topic.
    """
    # TODO(joshszep|DATAPIPE-2113): Update to use ConsumerGroup rather than SimpleConsumers
    return {
        topic: SimpleConsumer(
            client=kafka_client,
            group=None,
            topic=topic,
            auto_commit=False
        )
        for topic in topics
    }


def _build_topic_to_range_map(watermarks):
    """Builds a topic_to_range_map from a kafka get_topics_watermarks
    response"""
    return {
        topic: {
            partition: {
                'high': marks.highmark,
                'low': marks.lowmark
            }
            for partition, marks in watermarks_map.items()
        }
        for topic, watermarks_map in watermarks.items()
    }


def _build_topic_to_consumer_topic_state_map(watermarks):
    """Builds a topic_to_consumer_topic_state_map from a kafka
    get_topics_watermarks response"""
    return {
        topic: ConsumerTopicState({
            partition: int((marks.highmark + marks.lowmark) / 2)
            for partition, marks in watermarks_map.items()
        }, None)
        for topic, watermarks_map in watermarks.items()
    }


def _build_empty_topic_to_consumer_topic_state_map(topics):
    """Builds a topic_to_consumer_topic_state_map from a list of topics where
    all of the interior partition_offset_maps are empty"""
    return {
        topic: ConsumerTopicState({}, None)
        for topic in topics
    }


def _get_message_and_alter_range(
    topic_to_consumer_map,
    start_timestamp,
    topic_to_consumer_topic_state_map,
    topic_to_range_map,
    result_topic_to_consumer_topic_state_map
):
    """Create a consumer based on our topic_to_consumer_state_map, get a
    message, and based on that message's timestamp, adjust our topic ranges
    and maps with _update_ranges_for_message and
    _move_finished_topics_to_result_map"""
    for topic, consumer_topic_state in topic_to_consumer_topic_state_map.items():
        consumer = topic_to_consumer_map[topic]
        for partition, offset in consumer_topic_state.partition_offset_map.items():
            consumer.seek(
                offset=offset,
                partition=partition
            )
        response = consumer.get_message(
            timeout=0.1,
            block=True,
            get_partition_info=True,
        )
        if response is None:
            return
        offset, partition, timestamp = _get_update_info_from_simple_consumer_response(
            response
        )
        _update_ranges(
            topic=topic,
            offset=offset,
            partition=partition,
            timestamp=timestamp,
            start_timestamp=start_timestamp,
            topic_to_consumer_topic_state_map=topic_to_consumer_topic_state_map,
            topic_to_range_map=topic_to_range_map
        )
    _move_finished_topics_to_result_map(
        topic_to_consumer_topic_state_map,
        topic_to_range_map,
        result_topic_to_consumer_topic_state_map
    )


def _get_update_info_from_simple_consumer_response(response):
    (partition, (offset, raw_message)) = response
    # message is of type kafka.common.Message
    raw_message_bytes = raw_message.value
    unpacked_message = Envelope().unpack(packed_message=raw_message_bytes)
    timestamp = unpacked_message['timestamp']
    return offset, partition, timestamp


def _move_finished_topics_to_result_map(
    topic_to_consumer_topic_state_map,
    topic_to_range_map,
    result_topic_to_consumer_topic_state_map
):
    """Moves all "finished" (where max of topic range is equal to min)
    partitions to result_topic_to_consumer_topic_state_map for output, and
    removes them from the intermediate ones to stop getting messages from
    that partition."""
    for topic, topic_state_map in topic_to_consumer_topic_state_map.iteritems():
        partition_offset_map = topic_state_map.partition_offset_map
        for partition in partition_offset_map.keys():
            topic_range = topic_to_range_map[topic][partition]
            if topic_range['high'] == topic_range['low']:
                _pop_partition_offset_into_result_map(
                    partition,
                    partition_offset_map,
                    result_topic_to_consumer_topic_state_map,
                    topic
                )

    for topic in topic_to_consumer_topic_state_map.keys():
        consumer_topic_state_map = topic_to_consumer_topic_state_map[topic]
        if not consumer_topic_state_map.partition_offset_map:
            topic_to_consumer_topic_state_map.pop(topic)


def _pop_partition_offset_into_result_map(
        partition,
        partition_offset_map,
        result_topic_to_consumer_topic_state_map,
        topic
):
    result_topic_to_consumer_topic_state_map[
        topic
    ].partition_offset_map[partition] = partition_offset_map.pop(
        partition
    )


def _update_ranges(
    topic,
    offset,
    partition,
    timestamp,
    start_timestamp,
    topic_to_consumer_topic_state_map,
    topic_to_range_map
):
    """Updates our topic_to_range_maps and sets the new state of the
    topic_to_consumer_topic_state_map, based on start_timestamp"""

    partition_range_map = topic_to_range_map[topic]
    partition_range = partition_range_map[partition]
    if timestamp < start_timestamp:
        partition_range['low'] = offset + 1
    else:
        partition_range['high'] = offset
    new_mid = int((partition_range['high'] + partition_range['low']) / 2)
    partition_offset_map = topic_to_consumer_topic_state_map[topic].partition_offset_map
    partition_offset_map[partition] = new_mid
    logger.debug(
        "During start-date offset search got message from "
        "topic|offset|part: {}|{}|{}, new range: {}".format(
            topic, offset, partition, partition_range_map
        )
    )
