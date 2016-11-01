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

import clog

from data_pipeline.config import get_config
from data_pipeline.envelope import Envelope

logger = get_config().logger


class ClogWriter(object):

    def __init__(self):
        self.envelope = Envelope()

    def publish(self, message):
        try:
            clog.log_line(message.topic, self.envelope.pack(message, ascii_encoded=True))
        except:
            logger.error("Failed to scribe message - {}".format(str(message)))
