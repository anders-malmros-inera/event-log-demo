"""
Tests for container lifecycle control.
"""
import pytest
from unittest.mock import patch, MagicMock
from container_controller import (
    execute_docker_compose_command,
    get_container_status,
    stop_service,
    start_service,
    restart_service,
    pause_service,
    unpause_service
)


def test_execute_command_empty_service():
    """Given empty service name, return error."""
    success, message = execute_docker_compose_command("", "stop")
    assert success is False
    assert "cannot be empty" in message


def test_execute_command_invalid_action():
    """Given invalid action, return error."""
    success, message = execute_docker_compose_command("kafka", "destroy")
    assert success is False
    assert "Invalid action" in message


@patch('container_controller.subprocess.run')
@patch('container_controller.get_compose_file_path')
def test_execute_command_success(mock_path, mock_run):
    """Given valid service and action, execute docker compose command."""
    mock_path.return_value = '/docker-compose.yml'
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stderr = ""
    mock_run.return_value = mock_result
    
    success, message = execute_docker_compose_command("kafka", "stop")
    
    assert success is True
    assert "Successfully stopped kafka" in message
    mock_run.assert_called_once()
    call_args = mock_run.call_args[0][0]
    assert 'docker' in call_args
    assert 'compose' in call_args
    assert 'stop' in call_args
    assert 'kafka' in call_args


@patch('container_controller.subprocess.run')
@patch('container_controller.get_compose_file_path')
def test_execute_command_failure(mock_path, mock_run):
    """Given docker compose fails, return error message."""
    mock_path.return_value = '/docker-compose.yml'
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "Container not found"
    mock_run.return_value = mock_result
    
    success, message = execute_docker_compose_command("kafka", "stop")
    
    assert success is False
    assert "Failed to stop kafka" in message
    assert "Container not found" in message


@patch('container_controller.subprocess.run')
@patch('container_controller.get_compose_file_path')
def test_get_container_status_running(mock_path, mock_run):
    """Given container is running, return running status."""
    mock_path.return_value = '/docker-compose.yml'
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = '{"State": "running", "Name": "kafka"}'
    mock_run.return_value = mock_result
    
    status = get_container_status("kafka")
    
    assert status['status'] == 'running'
    assert 'kafka is running' in status['message']


def test_get_container_status_empty_service():
    """Given empty service name, return error."""
    status = get_container_status("")
    assert status['status'] == 'error'
    assert "cannot be empty" in status['message']


def test_stop_service():
    """Given service name, call execute_docker_compose_command with stop."""
    with patch('container_controller.execute_docker_compose_command') as mock_exec:
        mock_exec.return_value = (True, "Stopped")
        success, message = stop_service("flink")
        mock_exec.assert_called_once_with("flink", "stop")


def test_start_service():
    """Given service name, call execute_docker_compose_command with start."""
    with patch('container_controller.execute_docker_compose_command') as mock_exec:
        mock_exec.return_value = (True, "Started")
        success, message = start_service("flink")
        mock_exec.assert_called_once_with("flink", "start")


def test_restart_service():
    """Given service name, call execute_docker_compose_command with restart."""
    with patch('container_controller.execute_docker_compose_command') as mock_exec:
        mock_exec.return_value = (True, "Restarted")
        success, message = restart_service("kafka")
        mock_exec.assert_called_once_with("kafka", "restart")


def test_pause_service():
    """Given service name, call execute_docker_compose_command with pause."""
    with patch('container_controller.execute_docker_compose_command') as mock_exec:
        mock_exec.return_value = (True, "Paused")
        success, message = pause_service("cassandra")
        mock_exec.assert_called_once_with("cassandra", "pause")


def test_unpause_service():
    """Given service name, call execute_docker_compose_command with unpause."""
    with patch('container_controller.execute_docker_compose_command') as mock_exec:
        mock_exec.return_value = (True, "Unpaused")
        success, message = unpause_service("cassandra")
        mock_exec.assert_called_once_with("cassandra", "unpause")
