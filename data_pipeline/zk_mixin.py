# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import signal
import sys

import kazoo.client
import yelp_lib.config_loader
from kazoo.exceptions import LockTimeout
from kazoo.retry import KazooRetry

from data_pipeline.config import get_config

log = get_config().logger

KAZOO_CLIENT_DEFAULTS = {
    'timeout': 30
}

class ZKBatchMixin(object):
    """Add this to your batch definitions if you want your batch to interract with zookeeper."""

    def get_ecosystem(self):
        return open('/nail/etc/ecosystem').read().strip()

    def get_local_zk(self):
        path = get_config().zookeeper_discovery_path.format(ecosystem=self.get_ecosystem())
        """Get (with caching) the local zookeeper cluster definition."""
        return yelp_lib.config_loader.load(path, '/')

    def get_kazoo_client_for_cluster_def(self, cluster_def, **kwargs):
        """Get a KazooClient for a list of host-port pairs `cluster_def`."""
        host_string = ','.join('%s:%s' % (host, port) for host, port in cluster_def)

        for default_kwarg, default_value in KAZOO_CLIENT_DEFAULTS.iteritems():
            if default_kwarg not in kwargs:
                kwargs[default_kwarg] = default_value

        return kazoo.client.KazooClient(host_string, **kwargs)

    def get_kazoo_client(self, **kwargs):
        """Get a KazooClient for a local zookeeper cluster."""
        return self.get_kazoo_client_for_cluster_def(self.get_local_zk(), **kwargs)

    def setup_zk_lock(self):
        """
        Sets up zookeeper lock so that only one copy of the batch is run per cluster.
        This would make sure that data integrity is maintained (See DATAPIPE-309 for an example).
        Requires namespace to be set, and the batch to be a child of the base batch

        To use:
            call setup_zk_lock once you've set self.namespace
            call close_zk at the end of run (remember to set up signal handlers for keyboard interuptions
                 or crashes.)"""
        log.info("Making sure there's only one instance of the batch running against this data source.")
        retry_policy = KazooRetry(max_tries=3)
        self.zk_client = self.get_kazoo_client(command_retry=retry_policy)
        self.zk_client.start()
        self.lock = self.zk_client.Lock("/{} - {}".format(self.get_name(), self.namespace), self.namespace)
        try:
            self.lock.acquire(timeout=10)
        except LockTimeout:
            log.info("Already one instance running against this source! exit. See y/oneandonly for help.")
            self.close_zk()
            sys.exit(1)

    def close_zk(self):
        """ Clean up the zookeeper client."""
        if self.lock.is_acquired:
            log.info("Releasing the lock...")
            self.lock.release()
        log.info("Stopping zookeeper...")
        self.zk_client.stop()
        log.info("Closing zookeeper...")
        self.zk_client.close()
