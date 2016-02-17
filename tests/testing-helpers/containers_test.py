import subprocess

import pytest
from kafka import KafkaClient

from data_pipeline.testing_helpers.containers import Containers, ContainerUnavailableError

ZOOKEEPER = 'zookeeper'


@pytest.fixture()
def test_container():
    return Containers()


def test_compose_prefix(test_container):
    project_name = test_container.project
    expected_result = ("docker-compose "
                       "--file=/nail/home/pxu/pg/services/data_pipeline"
                       "/data_pipeline/testing_helpers/docker-compose.yml "
                       "--project-name={project_name}").format(project_name=project_name)
    actual_result = Containers.compose_prefix()
    assert expected_result == actual_result


def test_get_container_info_throws_exception():
    """
    Asserts that the method returns a custom exception when the container for
    the given project and service is not present.
    """
    with pytest.raises(ContainerUnavailableError):
        project = 'test_project'
        service = 'test_service'
        Containers.get_container_info(project, service)


def test_get_container_info(containers):
    """
    Asserts that when the specific container being queried is running then
    it returns the information about the specific container.
    """
    project = containers.project
    service = ZOOKEEPER
    container_info = Containers.get_container_info(project=project, service=service)
    assert container_info is not None
    assert container_info is not ContainerUnavailableError
    assert container_info['Labels'].get('com.docker.compose.service') == service


def test_get_container_ip_address_of_nonexistent_container(test_container):
    """
    Asserts that the ContainerUnavailableError is raised when trying to get
    an IP of a non-existent container.
    """
    with pytest.raises(ContainerUnavailableError):
        project = 'test_project'
        service = 'test_service'
        timeout_seconds = 1
        test_container.get_container_ip_address(
            project=project,
            service=service,
            timeout_seconds=timeout_seconds
        )


def test_get_container_ip(containers):
    """
    Asserts that when an existing container is queried for its IP it returns
    the specified projects and services IP address.
    """
    project = containers.project
    service = ZOOKEEPER
    timeout_seconds = 5
    actual_ip = Containers.get_container_ip_address(
        project=project,
        service=service,
        timeout_seconds=timeout_seconds
    )
    container_id = Containers.get_container_info(
        project=project,
        service=service
    )['Id']
    command = "docker inspect --format '{{{{ .NetworkSettings.IPAddress }}}}' {container_id}" \
        .format(container_id=container_id)
    expected_ip = subprocess.check_output([command], shell=True)

    assert expected_ip.rstrip() == actual_ip


def test_get_kafka_connection(containers):
    """
    Asserts that the method returns a working kafka client connection.
    """
    timeout_seconds = 1
    kafka_connection = containers.get_kafka_connection(timeout_seconds=timeout_seconds)
    assert isinstance(kafka_connection, KafkaClient)
