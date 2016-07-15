========
Usage
========

To use Data Pipeline Clientlib in a project::

    >>> import data_pipeline

To use a Consumer::

    >>> from data_pipeline.consumer import Consumer
    >>> from data_pipeline.expected_frequency import ExpectedFrequency
    >>> Consumer(
    ...     'test',
    ...     'bam',
    ...     ExpectedFrequency.constantly,
    ...     {'topic_name': None}
    ... ) # doctest: +ELLIPSIS
    <data_pipeline.consumer.Consumer object at 0x...>

And another thing::

    >>> from data_pipeline.envelope import Envelope
    >>> envelope = Envelope()
