# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cached_property import cached_property

from benchmarks.benchmarks import Benchmarks
from data_pipeline.envelope import Envelope
from data_pipeline.message import CreateMessage


class MessagesBenchmark(object):
    @cached_property
    def envelope(self):
        return Envelope()

    @cached_property
    def message(self):
        return CreateMessage(str('topic'), 10, bytes(10))

    def benchmark_messsage_creation(self):
        CreateMessage(str('topic'), 10, bytes(10))

    def benchmark_envelope_packing(self):
        self.envelope.pack(self.message)


if __name__ == '__main__':
    Benchmarks().execute_benchmark_class(MessagesBenchmark)
