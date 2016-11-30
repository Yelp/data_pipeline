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

import getpass
import os
import re
import subprocess
import time

import requests
from docker import Client
from kafka import KafkaClient
from kafka.common import KafkaUnavailableError

from data_pipeline.config import configure_from_dict
from data_pipeline.config import get_config


logger = get_config().logger


class ContainerUnavailableError(Exception):
    def __init__(self, project='unknown', service='unknown'):
        try:
            msg = open("logs/docker-compose.log", 'r').read()
        except IOError:
            msg = "no error log."
        Exception.__init__(self, "Container for project {0} and service {1} failed to start with {2}".format(
            project,
            service,
            msg
        ))


class Containers(object):
    """Context manager that defers to already running docker
    containers if available, and if not, runs new containers for the duration
    of tests.

    Examples:

        To use the data pipeline containers in another project, include a
        fixture in `conftest.py` that will launch kafka, schematizer and
        dependent containers, then configure the clientlib to use them::

            @pytest.yield_fixture(scope='session')
            def containers():
                with Containers() as containers:
                    yield containers

        You'll also need to install the data_pipeline testing_helpers extras,
        to get all the necessary dependencies.  The easiest way to do that is
        to include the following in your packages `requirements-dev.txt`::

            data_pipeline[testing_helpers]

        :meth:`create_kafka_topic` can be used to create a new kafka topic in
        the running container::

            @pytest.fixture
            def topic(containers):
                containers.create_kafka_topic(str('some-topic-name'))

        When set to `true`, the `LEAVE_CONTAINERS_RUNNING` environment variable
        will prevent this class from shutting down testing containers between
        runs::

            $ LEAVE_CONTAINERS_RUNNING=true py.test tests/

        If you leave containers running, you'll need to manually clean up.  If
        you add a make target as described below, that can be accomplished
        by running something like::

            $ $(make compose-prefix) kill && $(make compose-prefix) rm

        The `FORCE_FRESH_CONTAINERS` environment variable will force this class
        to stop any already-running containers, remove them, and start them
        again from scratch::

            $ FORCE_FRESH_CONTAINERS=true py.test tests/

        The `PULL_CONTAINERS` environment variable updates all of the underlying
        containers before starting containers, if it's necessary to start new
        containers::

            $ PULL_CONTAINERS=true py.test tests/

        The `OPEN_SOURCE_MODE` environment variable should be set when running in
        open source mode::

            $ OPEN_SOURCE_MODE=true py.test tests/

        When running tests using an automated system like Jenkins, both
        `FORCE_FRESH_CONTAINERS` and `PULL_CONTAINERS` should be set, so tests
        are always run against pristene containers and the most recently deployed
        version of the schematizer. A Makefile target can set these variables
        directly::

            test:
                # This will timeout after 15 minutes, in case there is a hang on jenkins
                PULL_CONTAINERS=true FORCE_FRESH_CONTAINERS=true timeout -9 900 tox $(REBUILD_FLAG)

        An "Archive Artifacts" step can be added to the automated build
        capturing the projects "logs/**/*", which will contain the projects
        docker-compose log.  Additional debugging logs for the test run can be
        captured by adding the following to `conftest.py`::

            logging.basicConfig(
                level=logging.DEBUG,
                filename='logs/test.log',
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
            )

        If you want to manually inspect the containers with docker-compose, you
        can get the preconfigured docker-compose command by running something
        like the following from within a virtualenv::

            python -c "from data_pipeline.testing_helpers.containers import Containers; print Containers.compose_prefix()"

        That will return something along the lines of::

            docker-compose --file=/nail/home/justinc/pg/data_pipeline/data_pipeline/testing_helpers/docker-compose.yml --project-name=datapipelinejustinc  # NOQA

        In this project, a make target wraps this, which allows
        users to run commands like::

            $(make compose-prefix) ps
    """

    @classmethod
    def compose_prefix(cls):
        """Returns a configured docker-compose command for the user and project
        """
        return "docker-compose {}".format(Containers()._compose_options)

    @classmethod
    def get_container_info(cls, project, service, docker_client=None):
        """Returns container information for the first container matching project
        and service.
        Raises ContainerUnavailableError if the container is unavailable.

        Args:
            project: Name of the project the container is hosting
            service: Name of the service that the container is hosting
            docker_client: The docker client object
        """
        if docker_client is None:
            docker_client = Client(version='auto')

        try:
            return next(
                c for c in docker_client.containers() if
                c['Labels'].get('com.docker.compose.project') == project and
                c['Labels'].get('com.docker.compose.service') == service
            )
        except StopIteration:
            raise ContainerUnavailableError(project=project, service=service)

    @classmethod
    def get_container_ip_address(cls, project, service, timeout_seconds=100):
        """Fetches the ip address assigned to the running container.
        Throws ContainerUnavailableError if the retry limit is reached.

        Args:
            project: Name of the project the container is hosting
            service: Name of the service that the container is hosting
            timeout_seconds: Retry time limit to wait for containers to start
                             Default in seconds: 60
        """
        docker_client = Client(version='auto')
        container_id = cls._get_container_id(project, service, docker_client, timeout_seconds)
        return docker_client.inspect_container(
            container_id
        )['NetworkSettings']['IPAddress']

    @classmethod
    def exec_command(cls, command, project, service, timeout_seconds=60):
        """Executes the command in the project and service container running under
        docker-compose.
        Throws ContainerUnavailableError if the retry limit is reached.

        Args:
            command: The command to be executed in the container
            project: Name of the project the container is hosting
            service: Name of the service that the container is hosting
            timeout_seconds: Retry time limit to wait for containers to start
                             Default in seconds: 60
        """
        docker_client = Client(version='auto')
        container_id = cls._get_container_id(project, service, docker_client, timeout_seconds)
        exec_id = docker_client.exec_create(container_id, command)['Id']
        docker_client.exec_start(exec_id)

    @classmethod
    def get_kafka_connection(cls, timeout_seconds=15):
        """Returns a kafka connection, waiting timeout_seconds for the container
        to come up.

        Args:
            timeout_seconds: Retry time (seconds) to get a kafka connection
        """
        end_time = time.time() + timeout_seconds
        logger.info("Getting connection to Kafka container on yocalhost")
        while end_time > time.time():
            try:
                return KafkaClient(get_config().cluster_config.broker_list)
            except KafkaUnavailableError:
                logger.info("Kafka not yet available, waiting...")
                time.sleep(0.1)
        raise KafkaUnavailableError()

    @classmethod
    def _get_container_id(cls, project, service, docker_client, timeout_seconds=60):
        end_time = time.time() + timeout_seconds
        while end_time > time.time():
            try:
                container_info = cls.get_container_info(
                    project,
                    service,
                    docker_client=docker_client
                )
                return container_info['Id']
            except ContainerUnavailableError:
                time.sleep(1)
        else:
            raise ContainerUnavailableError(project=project, service=service)

    def __init__(self, additional_compose_file=None, additional_services=None):
        # To resolve docker client server version mismatch issue.
        os.environ["COMPOSE_API_VERSION"] = "auto"
        dir_name = os.path.split(os.getcwd())[-1]
        self.project = "{}{}".format(
            re.sub(r'[^a-z0-9]', '', dir_name.lower()),
            getpass.getuser()
        )
        self.additional_compose_file = additional_compose_file

        self.services = ["zookeeper", "schematizer", "kafka"]

        if additional_services is not None:
            self.services.extend(additional_services)

        # This variable is meant to capture the running/not-running state of
        # the dependent testing containers when tests start running.  The idea
        # is, we'll only start and stop containers if they aren't already
        # running.  If they are running, we'll just use the ones that exist.
        # It takes a while to start all the containers, so when running lots of
        # tests, it's best to start them out-of-band and leave them up for the
        # duration of the session.
        self.containers_already_running = self._are_containers_already_running()

    def __enter__(self):
        if (
            not self.containers_already_running or
            self._is_envvar_set('FORCE_FRESH_CONTAINERS')
        ):
            self._start_containers()
        else:
            logger.info("Using running containers")

        self.use_testing_containers()

        return self

    def __exit__(self, type, value, traceback):
        if not (
            self.containers_already_running or
            self._is_envvar_set('LEAVE_CONTAINERS_RUNNING')
        ):
            # only stop containers that we started
            self._stop_containers()
        return False  # Don't Suppress Exception

    def create_kafka_topic(self, topic):
        """This method execs in the docker container because it's the only way to
        control how the topic is created.

        Args:
            topic (str): Topic name to create
        """
        conn = Containers.get_kafka_connection()
        if conn.has_metadata_for_topic(topic):
            return

        logger.info("Creating Fake Topic")
        if not isinstance(topic, str):
            raise ValueError("topic must be a str, it cannot be unicode")

        kafka_create_topic_command = (
            "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper zk:2181 "
            "--replication-factor 1 --partition 1 --topic {topic}"
        ).format(topic=topic)

        Containers.exec_command(kafka_create_topic_command, self.project, 'kafka')

        logger.info("Waiting for topic")
        conn.ensure_topic_exists(
            topic,
            timeout=get_config().topic_creation_wait_timeout
        )
        conn.close()
        logger.info("Topic Exists")
        assert conn.has_metadata_for_topic(topic)

    @property
    def _compose_options(self):
        file_name = ("docker-compose-opensource.yml"
                     if self._is_envvar_set('OPEN_SOURCE_MODE')
                     else "docker-compose.yml")
        compose_file = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                file_name
            )
        )
        if self.additional_compose_file:
            # The additional compose file need to be put in front.
            return "--file={} --file={} --project-name={}".format(
                self.additional_compose_file, compose_file, self.project
            )
        else:
            return "--file={} --project-name={}".format(compose_file, self.project)

    def use_testing_containers(self):
        """Configures the data pipeline clientlib to use the containers"""
        schematizer_ip = Containers.get_container_ip_address(self.project, 'schematizer')
        schematizer_host_and_port = "{}:8888".format(schematizer_ip)

        zookeeper_ip = Containers.get_container_ip_address(self.project, 'zookeeper')
        zookeeper_host_and_port = "{}:2181".format(zookeeper_ip)

        kafka_ip = Containers.get_container_ip_address(self.project, 'kafka')
        kafka_host_and_port = "{}:9092".format(kafka_ip)

        configure_from_dict(dict(
            schematizer_host_and_port=schematizer_host_and_port,
            kafka_zookeeper=zookeeper_host_and_port,
            kafka_broker_list=[kafka_host_and_port],
            should_use_testing_containers=True
        ))

    def _are_containers_already_running(self):
        return all(self._is_service_running(service) for service in self.services)

    def _is_service_running(self, service):
        return int(subprocess.call(
            "docker-compose {} ps {} | grep Up".format(self._compose_options, service),
            shell=True,
            stdout=subprocess.PIPE
        )) == 0

    def _start_containers(self):
        self._stop_containers()
        if self._is_envvar_set('PULL_CONTAINERS'):
            logger.info("Updating Containers")
            self._run_compose('pull')
        if self.additional_compose_file:
            logger.info("Building Containers")
            self._run_compose('build')
        logger.info("Starting Containers")
        self._run_compose('up', '-d', *self.services)
        self._wait_for_services()

    def _wait_for_services(self, timeout_seconds=60):
        self.use_testing_containers()
        self._wait_for_schematizer(timeout_seconds=90)
        self._wait_for_kafka(timeout_seconds)

    def _wait_for_schematizer(self, timeout_seconds):
        # wait for schematizer to pass health check
        end_time = time.time() + timeout_seconds
        logger.info("Waiting for schematizer to pass health check")
        count = 0
        while end_time > time.time():
            time.sleep(0.1)
            try:
                r = requests.get(
                    "http://{0}/v1/namespaces".format(get_config().schematizer_host_and_port)
                )
                if 200 <= r.status_code < 300:
                    count += 1
                    if count >= 2:
                        return
            except Exception:
                count = 0
            finally:
                logger.info("Schematizer not yet available, waiting...")
        raise ContainerUnavailableError(project='schematizer', service='schematizer')

    def _wait_for_kafka(self, timeout_seconds):
        # This will raise an exception after timeout_seconds, if it can't get
        # a connection
        Containers.get_kafka_connection(timeout_seconds)

    def _stop_containers(self):
        logger.info("Stopping containers")
        self._run_compose('kill')
        self._run_compose('rm', '--force')

    def _run_compose(self, *args):
        args = list(args)

        if not os.path.isdir('logs'):
            os.mkdir('logs')

        with open("logs/docker-compose.log", "a") as f:
            subprocess.call(
                ['docker-compose'] + self._compose_options.split() + args,
                stdout=f,
                stderr=subprocess.STDOUT
            )

    def _is_envvar_set(self, envvar):
        return os.getenv(envvar, 'false').lower() in ['t', 'true', 'y', 'yes']
