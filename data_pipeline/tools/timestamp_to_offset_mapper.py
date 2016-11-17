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

from uuid import UUID

from kafka_utils.util import offsets

from data_pipeline._fast_uuid import FastUUID
from data_pipeline.base_consumer import ConsumerTopicState
from data_pipeline.config import get_config
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency

logger = get_config().logger


def get_first_offset_at_or_after_start_timestamp(kafka_client, topics, start_timestamp):
    """Uses binary search to find the first offset that comes after start_timestamp for each
    topic in topics. If multiple items are present for a timestamp, the first one (closer to
    low_mark) is returned back.

    Outputs a result_topic_to_consumer_topic_state_map which can be used to set offsets

    :param kafka_client: kafka client to be used for getting watermarks and binary search.
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

    topic_to_consumer_topic_state_map = _build_topic_to_consumer_topic_state_map(watermarks)
    topic_to_range_map = _build_topic_to_range_map(watermarks)
    result_topic_to_consumer_topic_state_map = _build_empty_topic_to_consumer_topic_state_map(topics)

    _move_finished_topics_to_result_map(
        topic_to_consumer_topic_state_map,
        topic_to_range_map,
        result_topic_to_consumer_topic_state_map
    )
    while topic_to_consumer_topic_state_map:
        _get_message_and_alter_range(
            start_timestamp,
            topic_to_consumer_topic_state_map,
            topic_to_range_map,
            result_topic_to_consumer_topic_state_map
        )

    logger.info(
        "Got topic offsets based on start-date: {}".format(result_topic_to_consumer_topic_state_map)
    )
    return result_topic_to_consumer_topic_state_map


def _build_topic_to_range_map(watermarks):
    """Builds a topic_to_range_map from a kafka get_topics_watermarks response"""
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
    """Builds a topic_to_consumer_topic_state_map from a kafka get_topics_watermarks response"""
    return {
        topic: ConsumerTopicState({
            partition: int((marks.highmark + marks.lowmark) / 2)
            for partition, marks in watermarks_map.items()
        }, None)
        for topic, watermarks_map in watermarks.items()
    }


def _build_empty_topic_to_consumer_topic_state_map(topics):
    """Builds a topic_to_consumer_topic_state_map from a list of topics where all of the interior
    partition_offset_maps are empty"""
    return {
        topic: ConsumerTopicState({}, None)
        for topic in topics
    }


def _get_message_and_alter_range(
    start_timestamp,
    topic_to_consumer_topic_state_map,
    topic_to_range_map,
    result_topic_to_consumer_topic_state_map
):
    """Create a consumer based on our topic_to_consumer_state_map, get a message, and based
    on that message's timestamp, adjust our topic ranges and maps with _update_ranges_for_message and
    _move_finisehd_topics_to_result_map"""
    # We create a new consumer each time since it would otherwise require refactoring how
    # we consume from KafkaConsumerGroups (currently use an iterator, which doesn't support big jumps in offset)
    with Consumer(
        'data_pipeline_tailer_starting_offset_getter-{}'.format(
            str(UUID(bytes=FastUUID().uuid4()).hex)
        ),
        'bam',
        ExpectedFrequency.constantly,
        topic_to_consumer_topic_state_map
    ) as consumer:
        message = consumer.get_message(timeout=0.1, blocking=True)
        if message is None:
            return
        _update_ranges_for_message(
            message,
            start_timestamp,
            topic_to_consumer_topic_state_map,
            topic_to_range_map
        )
    _move_finished_topics_to_result_map(
        topic_to_consumer_topic_state_map,
        topic_to_range_map,
        result_topic_to_consumer_topic_state_map
    )


def _move_finished_topics_to_result_map(
    topic_to_consumer_topic_state_map,
    topic_to_range_map,
    result_topic_to_consumer_topic_state_map
):
    """Moves all "finished" (where max of topic range is equal to min) partitions
    to result_topic_to_consumer_topic_state_map for output, and removes them from the
    intermediate ones to stop getting messages from that partition"""
    for topic, consumer_topic_state_map in topic_to_consumer_topic_state_map.iteritems():
        for partition in consumer_topic_state_map.partition_offset_map.keys():
            topic_range = topic_to_range_map[topic][partition]
            if topic_range['high'] == topic_range['low']:
                result_topic_to_consumer_topic_state_map[topic].partition_offset_map[partition] = \
                    consumer_topic_state_map.partition_offset_map.pop(partition)

    for topic in topic_to_consumer_topic_state_map.keys():
        consumer_topic_state_map = topic_to_consumer_topic_state_map[topic]
        if not consumer_topic_state_map.partition_offset_map:
            topic_to_consumer_topic_state_map.pop(topic)


def _update_ranges_for_message(
    message,
    start_timestamp,
    topic_to_consumer_topic_state_map,
    topic_to_range_map
):
    """Given a message, updates our topic_to_range_maps and sets the new state of the
    topic_to_consumer_topic_state_map, based on start_timestamp"""
    topic = message.topic
    offset = message.kafka_position_info.offset
    partition = message.kafka_position_info.partition
    timestamp = message.timestamp

    if timestamp < start_timestamp:
        topic_to_range_map[topic][partition]['low'] = offset + 1
    else:
        topic_to_range_map[topic][partition]['high'] = offset
    new_mid = int(
        (
            topic_to_range_map[topic][partition]['high'] +
            topic_to_range_map[topic][partition]['low']
        ) / 2
    )
    topic_to_consumer_topic_state_map[topic].partition_offset_map[partition] = new_mid
    logger.debug(
        "During start-date offset search got message from "
        "topic|offset|part: {}|{}|{}, new range: {}".format(
            topic, offset, partition, topic_to_range_map[topic]
        )
    )
