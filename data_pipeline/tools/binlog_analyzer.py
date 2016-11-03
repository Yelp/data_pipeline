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
"""Use this like:
mysqlbinlog --read-from-remote-server --host 10.69.1.100 -u rbr_test \
    --stop-never --verbose --start-datetime="2015-03-08 00:45:00" \
    mysql-bin.000405 | ~/pypy-2.5.0-linux64/bin/pypy ~/binlog_analyzer.py \
    | ~/pypy-2.5.0-linux64/bin/pypy ~/compressed_stream_rotator.py
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import errno
import fileinput
import json
import re
import time


class BinlogParser(object):
    statement_to_type = {'INSERT INTO': 'insert', 'UPDATE': 'update', 'DELETE FROM': 'delete'}

    def __init__(self):
        self.timestamp = None
        self.header_timestamp = None

    def run(self):
        try:
            self._parse_binlog()
        except IOError as e:
            if e.errno == errno.EPIPE:
                # just stop if the pipe breaks
                pass
            else:
                raise

    def _parse_binlog(self):
        for line in fileinput.input():
            line = line.strip()
            self._process_line(line)

    def _process_line(self, line):
        if self._is_setting_timestamp(line):
            self._handle_timestamp_line(line)
        if self._is_header_line(line):
            self._handle_header_line(line)
        elif self._is_updating(line):
            self._handle_update_line(line)

    def _is_setting_timestamp(self, line):
        return line.startswith("SET TIMESTAMP=") and line.endswith("/*!*/;")

    def _handle_header_line(self, line):
        m = re.search("\\#(\\d+)\\s+(\\d+:\\d+:\\d+)\\s+server\\s+id\\s+\\d+", line)
        datetime_str = "%s %s" % (m.group(1), m.group(2))
        dt = datetime.datetime.strptime(datetime_str, '%y%m%d %H:%M:%S')
        new_header_timestamp = int(time.mktime(dt.timetuple()))
        self.header_timestamp = new_header_timestamp

    def _is_header_line(self, line):
        regex = "\\#(\\d+)\\s+(\\d+:\\d+:\\d+)\\s+server\\s+id\\s+\\d+.+(Update_rows|Write_rows|Delete_rows)"
        return re.search(regex, line) is not None

    def _handle_timestamp_line(self, line):
        m = re.search("SET\\ TIMESTAMP=(\\d+)/\\*!\\*/;", line)
        new_timestamp = int(m.group(1))
        self.timestamp = new_timestamp

    def _is_updating(self, line):
        return any(line.startswith("### %s " % s) for s in ['INSERT INTO', 'UPDATE', 'DELETE FROM'])

    def _handle_update_line(self, line):
        m = re.search("\\#\\#\\#\\ (DELETE\\ FROM|INSERT\\ INTO|UPDATE)\\ (.+)", line)
        statement_type = self.statement_to_type[m.group(1)]
        table = m.group(2)

        print json.dumps({
            'timestamp': self.header_timestamp,
            'statement_type': statement_type,
            'table': table
        })


if __name__ == "__main__":
    BinlogParser().run()
