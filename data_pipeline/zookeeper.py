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

import signal
import sys

import kazoo.client
import yaml
from cached_property import cached_property
from cached_property import cached_property_with_ttl
from kazoo.exceptions import LockTimeout
from kazoo.retry import KazooRetry

from data_pipeline.config import get_config

log = get_config().logger

KAZOO_CLIENT_DEFAULTS = {
    'timeout': 30
}


class ZK(object):
    """A class for zookeeper interactions"""

    @property
    def max_tries(self):
        return 3

    @cached_property
    def ecosystem(self):
        return open(get_config().ecosystem_file_path).read().strip()

    def __init__(self):
        retry_policy = KazooRetry(max_tries=self.max_tries)
        self.zk_client = self.get_kazoo_client(command_retry=retry_policy)
        self.zk_client.start()
        self.register_signal_handlers()

    @cached_property_with_ttl(ttl=2)
    def _local_zk(self):
        """Get (with caching) the local zookeeper cluster definition."""
        path = get_config().zookeeper_discovery_path.format(ecosystem=self.ecosystem)
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _get_kazoo_client_for_cluster_def(self, cluster_def, **kwargs):
        """Get a KazooClient for a list of host-port pairs `cluster_def`."""
        host_string = ','.join('%s:%s' % (host, port) for host, port in cluster_def)

        for default_kwarg, default_value in KAZOO_CLIENT_DEFAULTS.iteritems():
            if default_kwarg not in kwargs:
                kwargs[default_kwarg] = default_value

        return kazoo.client.KazooClient(host_string, **kwargs)

    def get_kazoo_client(self, **kwargs):
        """Get a KazooClient for a local zookeeper cluster."""
        return self._get_kazoo_client_for_cluster_def(self._local_zk, **kwargs)

    def close(self):
        """Clean up the zookeeper client."""
        log.info("Stopping zookeeper")
        self.zk_client.stop()
        log.info("Closing zookeeper")
        self.zk_client.close()

    def _exit_gracefully(self, sig, frame):
        self.close()
        if sig == signal.SIGINT:
            self.original_int_handler(sig, frame)
        elif sig == signal.SITERM:
            self.original_term_handler(sig, frame)

    def register_signal_handlers(self):
        self.original_int_handler = signal.getsignal(signal.SIGINT)
        self.original_term_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)


class ZKLock(ZK):
    """Sets up zookeeper lock so that only one copy of the batch is run per cluster.
    This would make sure that data integrity is maintained (See DATAPIPE-309 for an example).
    Use it as a context manager (i.e. with ZKLock(name, namespace))."""

    def __init__(self, name, namespace, timeout=10):
        super(ZKLock, self).__init__()
        self.lock = self.zk_client.Lock("/{} - {}".format(name, namespace), namespace)
        self.timeout = timeout
        self.failed = False

    def __enter__(self):
        try:
            self.lock.acquire(timeout=self.timeout)
        except LockTimeout:
            log.warning("Already one instance running against this source! exit. See y/oneandonly for help.")
            self.close()
            sys.exit(1)

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self.lock.is_acquired:
            log.info("Releasing the lock...")
            self.lock.release()
        super(ZKLock, self).close()
