# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import clog

from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope

logger = get_config().logger

# Since we are passing messages through scribe we don't support keys


class ClogWriter(object):

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.envelope = Envelope()

    def publish(self, message):
        if not self.dry_run:
            try:
                clog.log_line(message.topic, self.envelope.pack(message))
            except:
                logger.error("Failed to scribe message - {}".format(str(message)))
