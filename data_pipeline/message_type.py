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

from enum import Enum


class _EnumRepr(Enum):
    def __repr__(self):
        return '{}.{}({})'.format(
            self.__class__.__name__,
            self.name,
            self.value
        )


class MessageType(_EnumRepr):
    """Messages should be published primarily using the create, update, and
    delete types.  Refresh messages should only be produced if you know what
    you're doing, if in doubt, ask please.

    Attributes:
      create: when new data is created, the payload contains the contents of
        the new row
      update: when data is updated, payload contains the new content and
        previous_payload contains the old content of the row
      delete: when rows are removed, the payload contains the content of the
        row before removal
      refresh: refresh messages are used to intially populate a topic, they
        do not correspond to any particular data change
    """
    log = 0
    create = 1
    update = 2
    delete = 3
    refresh = 4


class _ProtectedMessageType(_EnumRepr):
    """Protected message types should generally be avoided.  The clientlib
    won't expose these messages to users, they're used internally only.

    Attributes:
      heartbeat: emitted periodically on low volume topics so auditing
        processes can differentiate between slow or stalled topics and
        topics without messages.
      monitor: monitor messages are used to count the number of messages
        produced/consumed by client in a given time frame
    """
    heartbeat = 5
    monitor = 6
    registration = 7
