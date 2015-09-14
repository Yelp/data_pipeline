# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import subprocess
import time
from contextlib import contextmanager

import requests
from docker import Client

from data_pipeline.config import get_config
from tests.helpers.config import reconfigure


logger = get_config().logger


class ContainerUnavailable(Exception):
    pass


class Containers(object):
    """Context manager that defers to already running docker
    containers if available, and if not, runs new containers for the duration
    of tests.
    """

    services = ["kafka", "schematizer"]

    @classmethod
    def get_container_info(cls, project, service, docker_client=Client(version='auto')):
        # intentionally letting this blow up if it can't find the container
        # - we can't do anything if the container doesn't exist
        return next(
            c for c in docker_client.containers() if
            c['Labels'].get('com.docker.compose.project') == project and
            c['Labels'].get('com.docker.compose.service') == service
        )

    @classmethod
    def get_container_ip_address(cls, project, service):
        docker_client = Client(version='auto')
        container_id = cls.get_container_info(
            project,
            service,
            docker_client
        )['Id']

        return docker_client.inspect_container(
            container_id
        )['NetworkSettings']['IPAddress']

    @classmethod
    def exec_command(cls, command, project, service):
        """Execs the command in the project and service container running under
        docker-compose.
        """
        docker_client = Client(version='auto')
        container_id = cls.get_container_info(
            project,
            service,
            docker_client=docker_client
        )['Id']

        exec_id = docker_client.exec_create(container_id, command)['Id']
        docker_client.exec_start(exec_id)

    def __init__(self):
        # This variable is meant to capture the running/not-running state of
        # the dependent testing containers when tests start running.  The idea
        # is, we'll only start and stop containers if they aren't already
        # running.  If they are running, we'll just use the ones that exist.
        # It takes a while to start all the containers, so when running lots of
        # tests, it's best to start them out-of-band and leave them up for the
        # duration of the session.
        self.containers_already_running = self._are_containers_already_running()

    def __enter__(self):
        if not self.containers_already_running:
            self._start_containers()
        else:
            logger.info("Using running containers")

        return self

    def __exit__(self, type, value, traceback):
        if not self.containers_already_running:
            # only stop containers that we started
            self._stop_containers()
        return False  # Don't Supress Exception

    @contextmanager
    def use_testing_containers(self, project='datapipeline'):
        schematizer_ip = Containers.get_container_ip_address(project, 'schematizer')
        schematizer_host_and_port = "{}:8888".format(schematizer_ip)

        zookeeper_ip = Containers.get_container_ip_address(project, 'zookeeper')
        zookeeper_host_and_port = "{}:2181".format(zookeeper_ip)

        kafka_ip = Containers.get_container_ip_address(project, 'kafka')
        kafka_host_and_port = "{}:9092".format(kafka_ip)

        with reconfigure(
            schematizer_host_and_port=schematizer_host_and_port,
            kafka_zookeeper=zookeeper_host_and_port,
            kafka_broker_list=[kafka_host_and_port]
        ):
            yield

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
        self._wait_for_schematizer(timeout_seconds)

    def _wait_for_schematizer(self, timeout_seconds):
        # wait for schematizer to pass health check
        end_time = time.time() + timeout_seconds
        logger.info("Waiting for schematizer to pass health check")
        count = 0
        while end_time > time.time():
            try:
                r = requests.get(
                    "http://{0}/status".format(get_config().schematizer_host_and_port)
                )
                if 200 <= r.status_code < 300:
                    count += 1
                    if count == 2:
                        return
            except:
                count = 0
            finally:
                logger.info("Schematizer not yet available, waiting...")
                time.sleep(0.1)
        raise ContainerUnavailable()

    def _stop_containers(self):
        logger.info("Stopping containers")
        subprocess.call(["docker-compose", "kill"])
        subprocess.call(["docker-compose", "rm", "--force"])
