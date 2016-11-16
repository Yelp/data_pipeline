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

from data_pipeline.schematizer_clientlib.models.refresh import RefreshStatus


class EmptyQueueError(Exception):
    def __init__(self, source_name):
        Exception.__init__(
            self, "Trying to pop from empty queue ({})".format(source_name)
        )


class PriorityRefreshQueue(object):
    """
    PriorityRefreshQueue orders paused/non-started jobs in the queue by:
    - higher priority > lower priority
    - paused status > not_started status
    - older > newer
    in that order of preference
    (i.e an older paused job will be beaten by any job with a higher priority)

    The only public ways to add/remove jobs from this queue are add_refreshes_to_queue and pop.

    We could implement this faster, but this is unnecessary as we have ample time between
    schematizer polls.

    Works for multiple sources within a single namespace but not across namespaces
    (since source_names are only unique within a namespace).

    Should only manage jobs that are either paused or non-started. This means when starting a job
    retrieved from peek, it should be popped from the queue.
    """

    def __init__(self):
        self.source_to_refresh_queue = {}
        self.refresh_ref = {}

    def _add_refresh_to_queue(self, refresh):
        if refresh.refresh_id not in self.refresh_ref:
            if refresh.source_name not in self.source_to_refresh_queue:
                self.source_to_refresh_queue[refresh.source_name] = []
            self.source_to_refresh_queue[refresh.source_name].append(
                refresh.refresh_id
            )
        self.refresh_ref[refresh.refresh_id] = refresh

    def _top_refresh(self, source_name):
        return self.refresh_ref[
            self.source_to_refresh_queue[source_name][0]
        ]

    def _sort_by_ascending_age(self, queue):
        return sorted(
            queue,
            key=lambda refresh_id: self.refresh_ref[refresh_id].created_at
        )

    def _sort_by_paused_first(self, queue):
        return sorted(
            queue,
            key=lambda refresh_id:
                (0 if self.refresh_ref[refresh_id].status == RefreshStatus.PAUSED else 1)
        )

    def _sort_by_descending_priority(self, queue):
        return sorted(
            queue,
            key=lambda refresh_id: self.refresh_ref[refresh_id].priority,
            reverse=True
        )

    def _sort_refresh_queue(self, queue):
        queue = self._sort_by_ascending_age(queue)
        queue = self._sort_by_paused_first(queue)
        return self._sort_by_descending_priority(queue)

    def add_refreshes_to_queue(self, refreshes):
        for refresh in refreshes:
            self._add_refresh_to_queue(refresh)

        for source, queue in self.source_to_refresh_queue.iteritems():
            self.source_to_refresh_queue[source] = self._sort_refresh_queue(queue)

    def peek(self):
        """Returns a dict of the top refresh for each source in the queue"""
        return {
            source_name: self._top_refresh(source_name)
            for source_name in self.source_to_refresh_queue
        }

    def pop(self, source_name):
        """Removes and returns the top refresh for the given source using its name
        (Note: source_name does not include its namespace)"""
        if source_name not in self.source_to_refresh_queue:
            raise EmptyQueueError(source_name)
        refresh_id = self.source_to_refresh_queue[source_name].pop(0)
        item = self.refresh_ref.pop(refresh_id)
        if not self.source_to_refresh_queue[source_name]:
            del self.source_to_refresh_queue[source_name]
        return item
