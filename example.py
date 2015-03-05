import data_pipeline
import locale
import time

from data_pipeline.consumer import Consumer
from data_pipeline.envelope import Envelope
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType


locale.setlocale(locale.LC_ALL, 'en_US')


def iter_timing(iterations=100000):
    start = time.time()
    for i in xrange(iterations):
        yield
    end = time.time()
    total_seconds = end - start
    pretty_iters = locale.format("%d", iterations, grouping=True)
    rate = locale.format("%.2f", round(iterations / total_seconds, 2), grouping=True)
    print "Total Time (%s iters): %s seconds" % (pretty_iters, total_seconds)
    print "Rate: %s/second" % rate

Consumer()

envelope = Envelope()
print envelope.schema

print "\n\nMessage Creation Time:"
for _ in iter_timing():
    message = Message(10, bytes(10), MessageType.create)


print "\n\nEnvelope Packing Time:"
for _ in iter_timing():
    packed = envelope.pack(message)

print packed.encode('hex')
