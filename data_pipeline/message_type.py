# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from enum import Enum


class MessageType(Enum):
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
    create = 1
    update = 2
    delete = 3
    refresh = 4


class _ProtectedMessageType(Enum):
    """Protected message types should generally be avoided.  The clientlib
    won't expose these messages to users, they're used internally only.

    Attributes:
      heartbeat: emitted periodically on low volume topics so auditing
        processes can differentiate between slow or stalled topics and
        topics without messages.
    """
    heartbeat = 5
