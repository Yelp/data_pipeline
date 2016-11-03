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

import os
import subprocess

import pytest
from kafka import KafkaClient

from data_pipeline.testing_helpers.containers import Containers
from data_pipeline.testing_helpers.containers import ContainerUnavailableError

ZOOKEEPER = 'zookeeper'


@pytest.fixture()
def test_container():
    return Containers()


def test_compose_prefix(test_container):
    file_name = ("docker-compose-opensource.yml"
                 if test_container._is_envvar_set('OPEN_SOURCE_MODE')
                 else "docker-compose.yml")
    project_name = test_container.project
    file_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    expected_result = ("docker-compose "
                       "--file={file_path}"
                       "/data_pipeline/testing_helpers/{file_name} "
                       "--project-name={project_name}").format(
        file_path=file_path,
        file_name=file_name,
        project_name=project_name)
    actual_result = Containers.compose_prefix()
    assert expected_result == actual_result


def test_get_container_info_throws_exception():
    """
    Asserts that the method returns a custom exception when the container for
    the given project and service is not present.
    """
    with pytest.raises(ContainerUnavailableError):
        Containers.get_container_info(project='test_project', service='test_service')


def test_get_container_info(containers):
    """
    Asserts that when the specific container being queried is running then
    it returns the information about the specific container.
    """
    container_info = Containers.get_container_info(
        project=containers.project,
        service=ZOOKEEPER)
    assert container_info is not None
    assert container_info is not ContainerUnavailableError
    assert container_info['Labels'].get('com.docker.compose.service') == ZOOKEEPER


def test_get_container_ip_address_of_nonexistent_container(test_container):
    """
    Asserts that the ContainerUnavailableError is raised when trying to get
    an IP of a non-existent container.
    """
    with pytest.raises(ContainerUnavailableError):
        test_container.get_container_ip_address(
            project='test_project',
            service='test_service',
            timeout_seconds=1
        )


def test_get_container_ip(containers):
    """
    Asserts that when an existing container is queried for its IP it returns
    the specified projects and services IP address.
    """
    actual_ip = Containers.get_container_ip_address(
        project=containers.project,
        service=ZOOKEEPER,
        timeout_seconds=5
    )
    container_id = Containers.get_container_info(
        project=containers.project,
        service=ZOOKEEPER
    )['Id']
    command = "docker inspect --format '{{{{ .NetworkSettings.IPAddress }}}}' {container_id}" \
        .format(container_id=container_id)
    expected_ip = subprocess.check_output([command], shell=True)

    assert expected_ip.rstrip() == actual_ip


def test_get_kafka_connection(containers):
    """
    Asserts that the method returns a working kafka client connection.
    """
    kafka_connection = containers.get_kafka_connection(timeout_seconds=1)
    assert isinstance(kafka_connection, KafkaClient)
