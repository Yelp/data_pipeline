from __future__ import absolute_import

from cached_property import cached_property
from data_pipeline.envelope import Envelope
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType
from benchmarks.benchmarks import Benchmarks


class MessagesBenchmark(object):
    @cached_property
    def envelope(self):
        return Envelope()

    @cached_property
    def message(self):
        return Message(10, bytes(10), MessageType.create)

    def benchmark_messsage_creation(self):
        Message(10, bytes(10), MessageType.create)

    def benchmark_envelope_packing(self):
        self.envelope.pack(self.message)


if __name__ == '__main__':
    Benchmarks().execute_benchmark_class(MessagesBenchmark)
