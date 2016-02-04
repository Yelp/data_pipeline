# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout

from data_pipeline.zk_mixin import ZKBatchMixin

class TestZKMixin(object):

    @property
    def fake_namespace(self):
        return "test_namespace"

    @property
    def fake_name(self):
        return "test_get_name"

    @property
    def lock_path(self):
        return "/{} - {}".format(self.fake_name, self.fake_namespace)

    @pytest.fixture
    def zk_client(self):
        return mock.Mock(autospec=KazooClient)

    @pytest.fixture
    def bad_lock(self):
        bad_lock = mock.Mock()
        bad_lock.acquire = mock.Mock(side_effect=LockTimeout('Test exception'))
        return bad_lock

    @pytest.fixture
    def locked_zk_client(self, bad_lock):
        zk_client = mock.Mock(autospec=(KazooClient))
        zk_client.Lock = mock.Mock(return_value=bad_lock)
        return zk_client

    @pytest.yield_fixture
    def patch_zk(self, zk_client):
        with mock.patch.object(
            ZKBatchMixin,
            'get_kazoo_client',
            return_value=zk_client
        ) as mock_zk:
            yield mock_zk

    @pytest.yield_fixture
    def locked_patch_zk(self, locked_zk_client):
        with mock.patch.object(
            ZKBatchMixin,
            'get_kazoo_client',
            return_value=locked_zk_client
        ) as mock_zk:
            yield mock_zk

    @pytest.fixture
    def zk_mixin_batch(
        self,
        zk_client,
        patch_zk
    ):
        zk_batch = ZKBatchMixin()
        zk_batch.namespace = self.fake_namespace
        zk_batch.get_name = mock.Mock(return_value=self.fake_name)
        return zk_batch

    @pytest.fixture
    def locked_zk_mixin_batch(
        self,
        locked_zk_client,
        locked_patch_zk
    ):
        zk_batch = ZKBatchMixin()
        zk_batch.namespace = self.fake_namespace
        zk_batch.get_name = mock.Mock(return_value=self.fake_name)
        return zk_batch

    @pytest.fixture
    def zk_mixin_batch_no_patch(self):
        zk_batch = ZKBatchMixin()
        zk_batch.namespace = self.fake_namespace
        zk_batch.get_name = mock.Mock(return_value=self.fake_name)
        return zk_batch

    def test_setup_lock_and_close(
        self,
        zk_mixin_batch,
        zk_client
    ):
        zk_mixin_batch.setup_zk_lock()
        zk_mixin_batch.close_zk()
        self._check_zk_lock(zk_client)

    def test_lock_exception(
        self,
        locked_zk_mixin_batch,
        locked_zk_client,
        bad_lock
    ):
        with mock.patch.object(
            sys,
            'exit'
        ) as mock_exit:
            locked_zk_mixin_batch.setup_zk_lock()
        assert bad_lock.acquire.call_count == 1
        mock_exit.assert_called_once_with(1)
        self._check_zk_lock(locked_zk_client)

    def test_get_kazoo_client(self, zk_mixin_batch_no_patch):
        zk_mixin_batch_no_patch.setup_zk_lock()
        assert zk_mixin_batch_no_patch.lock.path == self.lock_path
        zk_mixin_batch_no_patch.close_zk()
        assert len(zk_mixin_batch_no_patch.zk_client.hosts)

    def _check_zk_lock(self, zk_client):
        zk_client.Lock.assert_called_once_with(
            self.lock_path,
            self.fake_namespace
        )
        assert zk_client.start.call_count == 1
        assert zk_client.Lock.call_count == 1
        assert zk_client.stop.call_count == 1
        assert zk_client.close.call_count == 1
