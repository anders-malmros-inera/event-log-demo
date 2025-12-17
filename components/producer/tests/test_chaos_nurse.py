"""
Unit tests for chaos_nurse module.

Following Clean Code and TDD principles:
- Pure function testing where possible
- Thread-safety verification
- Clear test naming reflecting behavior
- Defensive testing for edge cases
"""

import pytest
from unittest.mock import patch, MagicMock
import time
import threading
from chaos_nurse import ChaosNurse


class TestChaosNurseEnableDisable:
    """Test chaos nurse enable/disable lifecycle."""
    
    def test_chaos_nurse_starts_disabled(self):
        """Given a new ChaosNurse, it should start disabled."""
        nurse = ChaosNurse()
        assert nurse.is_enabled() is False
    
    def test_enable_chaos_returns_success_message(self):
        """When enabling chaos, it should return success message."""
        nurse = ChaosNurse()
        result = nurse.enable()
        assert result['status'] == 'enabled'
        assert nurse.is_enabled() is True
    
    def test_disable_chaos_returns_success_message(self):
        """When disabling chaos, it should return success message."""
        nurse = ChaosNurse()
        nurse.enable()
        result = nurse.disable()
        assert result['status'] == 'disabled'
        assert nurse.is_enabled() is False
    
    def test_enable_when_already_enabled_is_idempotent(self):
        """Enabling an already-enabled chaos nurse should be idempotent."""
        nurse = ChaosNurse()
        nurse.enable()
        result = nurse.enable()
        assert result['status'] == 'enabled'
        assert nurse.is_enabled() is True
    
    def test_disable_when_already_disabled_is_idempotent(self):
        """Disabling an already-disabled chaos nurse should be idempotent."""
        nurse = ChaosNurse()
        result = nurse.disable()
        assert result['status'] == 'disabled'
        assert nurse.is_enabled() is False


class TestChaosNurseExecution:
    """Test chaos action execution."""
    
    @patch('chaos_nurse.stop_service')
    @patch('chaos_nurse.start_service')
    @patch('chaos_nurse.restart_service')
    def test_execute_chaos_action_when_enabled_calls_controller(self, mock_restart, mock_start, mock_stop):
        """When chaos is enabled, execute_chaos_action should invoke controller."""
        nurse = ChaosNurse()
        nurse.enable()
        
        # Execute multiple times to hit at least one action
        for _ in range(10):
            result = nurse.execute_chaos_action()
            if result['action_taken']:
                break
        
        # At least one of the controller functions should have been called
        assert mock_restart.called or mock_start.called or mock_stop.called
    
    @patch('chaos_nurse.stop_service')
    @patch('chaos_nurse.start_service')
    @patch('chaos_nurse.restart_service')
    def test_execute_chaos_action_when_disabled_does_nothing(self, mock_restart, mock_start, mock_stop):
        """When chaos is disabled, execute_chaos_action should not invoke controller."""
        nurse = ChaosNurse()
        result = nurse.execute_chaos_action()
        
        assert result['action_taken'] is False
        assert 'disabled' in result['message'].lower()
        mock_restart.assert_not_called()
        mock_start.assert_not_called()
        mock_stop.assert_not_called()
    
    @patch('chaos_nurse.restart_service', side_effect=Exception('Container error'))
    def test_execute_chaos_action_handles_controller_exceptions(self, mock_restart):
        """When controller raises exception, it should be caught and logged."""
        nurse = ChaosNurse()
        nurse.enable()
        
        # Force restart action by mocking random choice
        with patch('chaos_nurse.random.choice', side_effect=[('kafka', 'restart')]):
            result = nurse.execute_chaos_action()
        
        # Should return error indication
        assert result['action_taken'] is False
        assert 'error' in result['message'].lower() or 'exception' in result['message'].lower()


class TestChaosNurseHistory:
    """Test action history tracking."""
    
    @patch('chaos_nurse.restart_service')
    def test_action_history_records_successful_actions(self, mock_restart):
        """Successful chaos actions should be recorded in history."""
        nurse = ChaosNurse()
        nurse.enable()
        
        # Force a specific action
        with patch('chaos_nurse.random.choice', return_value=('kafka', 'restart')):
            nurse.execute_chaos_action()
        
        status = nurse.get_status()
        assert len(status['history']) == 1
        assert status['history'][0]['service'] == 'kafka'
        assert status['history'][0]['action'] == 'restart'
        assert 'timestamp' in status['history'][0]
    
    @patch('chaos_nurse.restart_service')
    def test_action_history_bounded_to_max_20_entries(self, mock_restart):
        """Action history should be bounded to maximum 20 entries."""
        nurse = ChaosNurse()
        nurse.enable()
        
        # Execute 25 actions
        for i in range(25):
            with patch('chaos_nurse.random.choice', return_value=('kafka', 'restart')):
                nurse.execute_chaos_action()
        
        status = nurse.get_status()
        assert len(status['history']) == 20  # Should be capped at 20
    
    def test_get_status_returns_complete_information(self):
        """get_status should return enabled state and history."""
        nurse = ChaosNurse()
        nurse.enable()
        
        status = nurse.get_status()
        assert 'enabled' in status
        assert 'history' in status
        assert status['enabled'] is True
        assert isinstance(status['history'], list)


class TestChaosNurseThreadSafety:
    """Test thread-safety of chaos nurse operations."""
    
    def test_concurrent_enable_disable_is_thread_safe(self):
        """Multiple threads enabling/disabling should not cause race conditions."""
        nurse = ChaosNurse()
        errors = []
        
        def toggle_chaos():
            try:
                for _ in range(10):
                    nurse.enable()
                    nurse.disable()
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=toggle_chaos) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        # Final state should be consistent
        assert nurse.is_enabled() in [True, False]
    
    @patch('chaos_nurse.restart_service')
    def test_concurrent_execution_is_thread_safe(self, mock_restart):
        """Multiple threads executing chaos actions should not cause race conditions."""
        nurse = ChaosNurse()
        nurse.enable()
        errors = []
        
        def execute_multiple():
            try:
                for _ in range(5):
                    with patch('chaos_nurse.random.choice', return_value=('kafka', 'restart')):
                        nurse.execute_chaos_action()
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=execute_multiple) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        status = nurse.get_status()
        # History should contain at most 20 entries despite concurrent access
        assert len(status['history']) <= 20


class TestChaosNurseIntegration:
    """Integration tests for chaos nurse with container controller."""
    
    @patch('chaos_nurse.stop_service')
    @patch('chaos_nurse.start_service')
    @patch('chaos_nurse.restart_service')
    def test_chaos_nurse_targets_correct_services(self, mock_restart, mock_start, mock_stop):
        """Chaos nurse should only target allowed services."""
        nurse = ChaosNurse()
        nurse.enable()
        
        # Execute many times to ensure we hit all services
        for _ in range(50):
            nurse.execute_chaos_action()
        
        # Check that only valid services were called
        valid_services = {'kafka', 'flink', 'cassandra', 'flink-taskmanager'}
        for call in mock_restart.call_args_list + mock_start.call_args_list + mock_stop.call_args_list:
            service_name = call[0][0]
            assert service_name in valid_services
