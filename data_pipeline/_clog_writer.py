# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import clog

from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope

logger = get_config().logger


class ClogWriter(object):

    def __init__(self):
        self.envelope = Envelope()

    def publish(self, message):
        try:
            clog.log_line(message.topic, self.envelope.pack(message))
        except:
            logger.error("Failed to scribe message - {}".format(str(message)))
