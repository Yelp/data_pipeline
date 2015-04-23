import data_pipeline
import locale
import time

from data_pipeline.consumer import Consumer
from data_pipeline.envelope import Envelope
from data_pipeline.message import Message
from data_pipeline.message_type import MessageType


locale.setlocale(locale.LC_ALL, 'en_US')


envelope = Envelope()
print envelope.schema

message = Message(10, bytes(10), MessageType.create)
packed = envelope.pack(message)

print packed.encode('hex')
