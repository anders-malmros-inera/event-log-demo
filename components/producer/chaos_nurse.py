"""
Chaos Nurse - Intermittently brings down, starts, and restarts component instances.

Provides controlled chaos engineering for demonstration purposes.
Actions occur approximately every 10 seconds when enabled.
"""
import random
import threading
import time
from typing import Dict, List
from container_controller import start_service, restart_service, pause_service, unpause_service, get_container_status


class ChaosNurse:
    """
    Chaos engineering component that randomly manipulates service lifecycle.
    
    Design: Tell, don't ask - components tell nurse what services to target.
    Pure decision logic - no hidden state in action selection.
    """
    
    def __init__(self, target_services: List[str]):
        self.target_services = target_services
        self.enabled = False
        self.action_history = []
        self.max_history = 20
        self._lock = threading.Lock()
    
    def is_enabled(self) -> bool:
        """Check if chaos nurse is currently active."""
        with self._lock:
            return self.enabled
    
    def enable(self):
        """Enable chaos nurse actions."""
        with self._lock:
            self.enabled = True
            self._add_history_entry({
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "action": "enabled",
                "service": "system",
                "result": "Chaos Nurse activated"
            })
        return {"status": "enabled"}
    
    def disable(self):
        """Disable chaos nurse actions."""
        with self._lock:
            self.enabled = False
            self._add_history_entry({
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "action": "disabled",
                "service": "system",
                "result": "Chaos Nurse deactivated"
            })
        return {"status": "disabled"}
    
    def _add_history_entry(self, entry: Dict):
        """Add entry to history with bounded size (thread-unsafe, call with lock)."""
        if not isinstance(entry, dict):
            return
        
        self.action_history.append(entry)
        # Bound history size to prevent unbounded growth
        if len(self.action_history) > self.max_history:
            self.action_history = self.action_history[-self.max_history:]
    
    def get_status(self) -> Dict:
        """Get current status and recent action history."""
        with self._lock:
            # Defensive copy to prevent external mutation
            return {
                "enabled": self.enabled,
                "target_services": list(self.target_services) if self.target_services else [],
                "recent_actions": list(self.action_history[-10:])  # Last 10 actions, bounded
            }
    
    def execute_chaos_action(self) -> Dict:
        """Execute a random chaos action on a random target service."""
        # Guard: check if enabled (with lock)
        if not self.is_enabled():
            return {
                "action_taken": False,
                "message": "Chaos Nurse is disabled"
            }
        
        with self._lock:
            # Guard: validate target services exist
            if not self.target_services or len(self.target_services) == 0:
                return {
                    "action_taken": False,
                    "message": "No target services configured"
                }
        
        # Select random service
        try:
            service = random.choice(self.target_services)
        except IndexError:
            return {
                "action_taken": False,
                "message": "Failed to select random service"
            }
        
        # Check current service status to determine valid actions
        status_info = get_container_status(service)
        current_state = status_info.get('status', 'unknown').lower()
        
        # Defensive: determine valid actions based on current state
        # Running services: can pause (temporarily freeze) or restart (quick recovery)
        # Paused services: can unpause to restore
        # Stopped/exited services: can start
        # Other states: skip to avoid invalid operations
        if current_state in ['running', 'up']:
            valid_actions = ['pause', 'restart']
        elif current_state in ['paused']:
            valid_actions = ['unpause']
        elif current_state in ['stopped', 'exited', 'created']:
            valid_actions = ['start']
        else:
            return {
                "action_taken": False,
                "message": f"Service {service} in state '{current_state}' - no safe action available"
            }
        
        # Select random valid action
        try:
            action = random.choice(valid_actions)
        except IndexError:
            return {
                "action_taken": False,
                "message": f"No valid actions for service {service} in state '{current_state}'"
            }
        
        # Execute action with error handling
        action_map = {
            'pause': pause_service,
            'unpause': unpause_service,
            'start': start_service,
            'restart': restart_service
        }
        
        try:
            action_func = action_map.get(action)
            if not action_func:
                return {
                    "action_taken": False,
                    "message": f"Unknown action: {action}"
                }
            
            success, message = action_func(service)
            
            # Record action in history
            with self._lock:
                self._add_history_entry({
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "action": action,
                    "service": service,
                    "success": success,
                    "result": message
                })
            
            return {
                "action_taken": True,
                "executed": True,
                "service": service,
                "action": action,
                "success": success,
                "message": message
            }
        except Exception as e:
            error_msg = f"Exception during chaos action: {str(e)}"
            print(error_msg, flush=True)
            return {
                "action_taken": False,
                "message": error_msg
            }
    
    def clear_history(self):
        """Clear action history."""
        with self._lock:
            self.action_history = []


# Global instance
chaos_nurse = None


def get_chaos_nurse() -> ChaosNurse:
    """Get or create the global chaos nurse instance."""
    global chaos_nurse
    if chaos_nurse is None:
        # Target services that are safe to restart during demo
        target_services = ['kafka', 'flink', 'cassandra']
        chaos_nurse = ChaosNurse(target_services)
    return chaos_nurse
