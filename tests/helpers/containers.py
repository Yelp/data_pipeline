# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import subprocess
import time

import requests

from data_pipeline.config import get_config


logger = get_config().logger


class ContainerUnavailable(Exception):
    pass


class Containers(object):
    """Context manager that defers to already running docker
    containers if available, and if not, runs new containers for the duration
    of tests.
    """

    services = ["kafka", "schematizer"]

    def __init__(self):
        self.containers_already_running = self._are_containers_already_running()

    def __enter__(self):
        if not self.containers_already_running:
            self._start_containers()
        else:
            logger.info("Using running containers")

    def __exit__(self, type, value, traceback):
        if not self.containers_already_running:
            self._stop_containers()
        return False  # Don't Supress Exception

    def _are_containers_already_running(self):
        return all(self._is_service_running(service) for service in self.services)

    def _is_service_running(self, service):
        return int(subprocess.call(
            "docker-compose ps {0} | grep Up".format(service),
            shell=True
        )) == 0

    def _start_containers(self):
        self._stop_containers()
        logger.info("Starting Containers")
        subprocess.Popen(["docker-compose", "up"] + self.services)
        self._wait_for_services()

    def _wait_for_services(self, timeout_seconds=15):
        # wait for schematizer to pass health check
        end_time = time.time() + timeout_seconds
        logger.info("Waiting for schematizer to pass health check")
        while end_time > time.time():
            try:
                r = requests.get(
                    "http://{0}/status".format(get_config().schematizer_host_and_port)
                )
                if 200 <= r.status_code < 300:
                    return
            except:
                pass
            finally:
                logger.info("Schematizer not yet available, waiting...")
                time.sleep(0.1)
        raise ContainerUnavailable()

    def _stop_containers(self):
        logger.info("Stopping containers")
        subprocess.call(["docker-compose", "kill"])
        subprocess.call(["docker-compose", "rm", "--force"])
