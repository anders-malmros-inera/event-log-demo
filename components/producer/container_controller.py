"""
Container lifecycle management for demo scenarios.

Provides pure functions to start/stop/restart containers using docker-compose.
Follows defensive coding with explicit error handling.
"""
import subprocess
import os
from typing import Dict, Tuple


def get_compose_file_path() -> str:
    """
    Get the path to docker-compose.yml.
    Tries to find it relative to the module or uses env variable.
    """
    env_path = os.environ.get('COMPOSE_FILE', '/docker-compose.yml')
    if os.path.exists(env_path):
        return env_path
    return '/docker-compose.yml'  # Default for Docker context


def get_action_past_tense(action: str) -> str:
    """Get past tense form of action verb."""
    past_tense = {
        'stop': 'stopped',
        'start': 'started',
        'restart': 'restarted',
        'pause': 'paused',
        'unpause': 'unpaused'
    }
    return past_tense.get(action, f"{action}ed")


def execute_docker_compose_command(service: str, action: str) -> Tuple[bool, str]:
    """
    Execute docker-compose command for a service.
    
    Args:
        service: Service name (e.g., 'flink', 'kafka', 'producer')
        action: Action to perform ('stop', 'start', 'restart', 'pause', 'unpause')
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    if not service:
        return False, "Service name cannot be empty"
    
    if action not in ['stop', 'start', 'restart', 'pause', 'unpause']:
        return False, f"Invalid action: {action}"
    
    compose_file = get_compose_file_path()
    
    try:
        # Get the directory containing the compose file for working directory
        import os
        compose_dir = os.path.dirname(compose_file) or '/'
        project_name = os.environ.get('COMPOSE_PROJECT_NAME', 'event-log-demo')
        
        cmd = ['docker', 'compose', '-p', project_name, '-f', compose_file, action, service]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=compose_dir
        )
        
        if result.returncode == 0:
            action_past = get_action_past_tense(action)
            return True, f"Successfully {action_past} {service}"
        else:
            return False, f"Failed to {action} {service}: {result.stderr}"
            
    except subprocess.TimeoutExpired:
        return False, f"Timeout executing {action} on {service}"
    except FileNotFoundError:
        return False, "Docker compose not found. Is Docker installed?"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def get_container_status(service: str) -> Dict[str, str]:
    """
    Get the status of a container.
    
    Args:
        service: Service name
    
    Returns:
        Dict with 'status' and 'message' keys
    """
    if not service:
        return {"status": "error", "message": "Service name cannot be empty"}
    
    compose_file = get_compose_file_path()
    
    try:
        import os
        compose_dir = os.path.dirname(compose_file) or '/'
        project_name = os.environ.get('COMPOSE_PROJECT_NAME', 'event-log-demo')
        
        cmd = ['docker', 'compose', '-p', project_name, '-f', compose_file, 'ps', '--all', '--format', 'json', service]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            cwd=compose_dir
        )
        
        if result.returncode == 0 and result.stdout.strip():
            # Parse JSON output to get status
            import json
            try:
                data = json.loads(result.stdout)
                state = data.get('State', 'unknown')
                return {"status": state, "message": f"{service} is {state}"}
            except json.JSONDecodeError:
                return {"status": "unknown", "message": "Could not parse status"}
        else:
            return {"status": "not_found", "message": f"Service {service} not found"}
            
    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Timeout getting status"}
    except Exception as e:
        return {"status": "error", "message": f"Error: {str(e)}"}


def stop_service(service: str) -> Tuple[bool, str]:
    """Stop a service."""
    return execute_docker_compose_command(service, 'stop')


def start_service(service: str) -> Tuple[bool, str]:
    """Start a service."""
    return execute_docker_compose_command(service, 'start')


def restart_service(service: str) -> Tuple[bool, str]:
    """Restart a service."""
    return execute_docker_compose_command(service, 'restart')


def pause_service(service: str) -> Tuple[bool, str]:
    """Pause a service (suspend execution)."""
    return execute_docker_compose_command(service, 'pause')


def unpause_service(service: str) -> Tuple[bool, str]:
    """Unpause a service (resume execution)."""
    return execute_docker_compose_command(service, 'unpause')
