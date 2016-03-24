# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from yelp_lib.classutil import cached_property

from data_pipeline.config import get_config
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer

class IntrospectorBatch(object):
    """The Data Pipeline Introspector provides information in to the current
    state of the data pipeline using the schematizer, zookeeper, and kafka.

    This is the base class of all of the individual introspection batches.
    """

    @cached_property
    def schematizer(self):
        return get_schematizer()

    @cached_property
    def config(self):
        return get_config()

    @classmethod
    def add_parser(cls, subparsers):
        raise NotImplementedError
