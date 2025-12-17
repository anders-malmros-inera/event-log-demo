"""
Tests for control API endpoints.
"""
import pytest
from unittest.mock import patch


def test_control_endpoint_stop_flink():
    """Given POST /control/flink/stop, stop the flink service."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    
    with patch('app.stop_service') as mock_stop:
        mock_stop.return_value = (True, "Successfully stopped flink")
        
        response = client.post('/control/flink/stop')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['service'] == 'flink'
        assert data['action'] == 'stop'
        mock_stop.assert_called_once_with('flink')


def test_control_endpoint_start_kafka():
    """Given POST /control/kafka/start, start the kafka service."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    
    with patch('app.start_service') as mock_start:
        mock_start.return_value = (True, "Successfully started kafka")
        
        response = client.post('/control/kafka/start')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['service'] == 'kafka'
        assert data['action'] == 'start'


def test_control_endpoint_invalid_service():
    """Given invalid service name, return 400 error."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    response = client.post('/control/invalid-service/stop')
    
    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'not allowed' in data['message']


def test_control_endpoint_invalid_action():
    """Given invalid action, return 400 error."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    response = client.post('/control/flink/destroy')
    
    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'not allowed' in data['message']


def test_control_endpoint_pause_cassandra():
    """Given POST /control/cassandra/pause, pause the cassandra service."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    
    with patch('app.pause_service') as mock_pause:
        mock_pause.return_value = (True, "Successfully paused cassandra")
        
        response = client.post('/control/cassandra/pause')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['action'] == 'pause'


def test_control_endpoint_service_failure():
    """Given service control fails, return 500 with error message."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    
    with patch('app.stop_service') as mock_stop:
        mock_stop.return_value = (False, "Container not found")
        
        response = client.post('/control/flink/stop')
        
        assert response.status_code == 500
        data = response.get_json()
        assert data['success'] is False
        assert 'Container not found' in data['message']


def test_status_endpoint_kafka():
    """Given GET /control/kafka/status, return kafka status."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    
    with patch('app.get_container_status') as mock_status:
        mock_status.return_value = {"status": "running", "message": "kafka is running"}
        
        response = client.get('/control/kafka/status')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['status'] == 'running'
        mock_status.assert_called_once_with('kafka')


def test_status_endpoint_invalid_service():
    """Given invalid service name for status, return 400 error."""
    import sys
    sys.path.insert(0, '/app')
    from app import app
    
    client = app.test_client()
    response = client.get('/control/invalid-service/status')
    
    assert response.status_code == 400
    data = response.get_json()
    assert 'error' in data
