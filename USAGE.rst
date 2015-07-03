========
Usage
========

To use Data Pipeline Clientlib in a project::

    >>> import data_pipeline

To use a Consumer::

    >>> from data_pipeline.consumer import Consumer
    >>> Consumer('test', {}) # doctest: +ELLIPSIS
    <data_pipeline.consumer.Consumer object at 0x...>

And another thing::

    >>> from data_pipeline.envelope import Envelope
    >>> envelope = Envelope()
