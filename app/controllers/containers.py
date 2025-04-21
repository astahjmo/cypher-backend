import logging
import asyncio
import json
# Removed FastAPI imports (except WebSocket related for now)
from fastapi import WebSocket, WebSocketDisconnect, Request # Keep Request for background task state access
from starlette.websockets import WebSocketState # Keep for ConnectionManager
from typing import List, Set, Dict, Optional, Any, AsyncGenerator # Added AsyncGenerator
# Removed Database import
from docker.errors import DockerException, NotFound as DockerNotFound # Import specific Docker NotFound

# Import models from new locations
from models.container.db_models import ContainerRuntimeConfig, PortMapping # DB Model
from models.auth.db_models import User # DB Model
from models.base import PyObjectId # Base Model
from models.github.db_models import RepositoryConfig # DB Model

# Import View Models needed for function signatures if returning specific types
from models.container.api_models import ContainerStatusInfoView, ScaleRequestView, ScaleResponseView

# Import services and repositories directly (classes, not dependency functions)
from services.docker_service import docker_service
from repositories.container_runtime_config_repository import ContainerRuntimeConfigRepository
from repositories.repository_config_repository import RepositoryConfigRepository
from repositories.build_status_repository import BuildStatusRepository
# Removed db_service import
# Removed auth dependency import

logger = logging.getLogger(__name__)

# Removed router definition

# --- Connection Manager for WebSockets (Remains Async, Stays in Controller for now) ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(f"WebSocket connected: {websocket.client}. Total connections: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            removed = websocket in self.active_connections
            self.active_connections.discard(websocket)
        if removed:
            logger.info(f"WebSocket explicitly disconnected and removed: {websocket.client}. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        async with self._lock:
            if not self.active_connections:
                return
            connections_to_send = list(self.active_connections)

        if not connections_to_send:
            return

        tasks = [connection.send_text(message) for connection in connections_to_send]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_send_count = 0
        for i, result in enumerate(results):
            connection = connections_to_send[i]
            if isinstance(result, Exception):
                 failed_send_count += 1
                 if isinstance(result, RuntimeError) and "Cannot call 'send' once a close message has been sent" in str(result):
                     logger.warning(f"Send failed for WebSocket {connection.client} (already closed).")
                 elif isinstance(result, WebSocketDisconnect):
                      logger.warning(f"Send failed for WebSocket {connection.client} (disconnected).")
                 else:
                     logger.error(f"Error broadcasting to WebSocket {connection.client}: {type(result).__name__} - {result}", exc_info=False)

manager = ConnectionManager()

# --- Background Task (Remains Async, Stays in Controller for now) ---
async def broadcast_status_updates(repo_config_repo: RepositoryConfigRepository):
    """
    Periodically fetches status of configured repos and existing containers,
    then broadcasts the combined list to connected WebSocket clients.
    """
    while True:
        connection_count = 0
        try:
            async with manager._lock:
                connection_count = len(manager.active_connections)

            if connection_count > 0:
                logger.debug("Broadcast task running: Fetching configured repos and container statuses...")

                def get_combined_status_sync():
                    final_status_list: List[Dict] = [] # Return dicts for now
                    try:
                        # 1. Get status of all *existing* managed containers from Docker
                        all_docker_statuses_list = docker_service.list_managed_containers()
                        logger.debug(f"Broadcast Sync: Found {len(all_docker_statuses_list)} docker statuses.")
                        docker_status_map: Dict[str, Dict] = {
                            status_info['repo_full_name']: status_info
                            for status_info in all_docker_statuses_list
                        }

                        # 2. Get all configured repositories using the injected repo instance
                        all_configs = repo_config_repo.find_all() # Use the injected repo
                        logger.debug(f"Broadcast Sync: Found {len(all_configs)} configured repos.")
                        configured_repo_names = {config.repo_full_name for config in all_configs}

                        processed_repos = set()

                        # 3. Process repos found in Docker
                        for repo_name, docker_status in docker_status_map.items():
                            # Directly use the dict from docker_service for now
                            final_status_list.append(docker_status)
                            processed_repos.add(repo_name)

                        # 4. Add configured repos that weren't found in Docker
                        for repo_name in configured_repo_names:
                            if repo_name not in processed_repos:
                                # Create a basic dict structure
                                status_dict = {
                                    "repo_full_name": repo_name,
                                    "running": 0, "stopped": 0, "paused": 0,
                                    "memory_usage_mb": 0.0, "cpu_usage_percent": 0.0,
                                    "containers": []
                                }
                                final_status_list.append(status_dict)

                        logger.debug(f"Broadcast Sync: Combined list size: {len(final_status_list)}")
                        return final_status_list
                    except Exception as e:
                        logger.error(f"Error during synchronous status fetching for broadcast: {e}", exc_info=True)
                        return [] # Return empty list on error

                # Run the synchronous data fetching in a thread
                combined_status_list = await asyncio.to_thread(get_combined_status_sync)

                # Proceed only if data fetching was successful
                if combined_status_list is not None: # Check if fetch didn't fail
                    # No need to convert to View models here, send raw dicts
                    message = json.dumps(combined_status_list)
                    logger.debug(f"Broadcasting status update: {message[:200]}...") # Log snippet
                    await manager.broadcast(message)
                else: # Log if fetch failed
                     logger.warning("Broadcast task: Failed to fetch combined status, skipping broadcast cycle.")

        except Exception as e:
            logger.error(f"Error in broadcast_status_updates task loop: {e}", exc_info=True)
        finally:
             await asyncio.sleep(5)


# --- Controller Logic Functions (called by views) ---

async def get_container_status_list_logic(
    user_id: PyObjectId,
    repo_config_repo: RepositoryConfigRepository
) -> List[Dict]: # Return list of dicts matching ContainerStatusInfoView structure
    """Controller logic to fetch combined container status."""
    final_status_list: List[Dict] = []
    try:
        # Run synchronous DB/Docker calls in threads
        def get_data_sync():
            _configured_repos = repo_config_repo.find_by_user(user_id)
            _all_docker_statuses = docker_service.list_managed_containers()
            return _configured_repos, _all_docker_statuses

        configured_repos, all_docker_statuses_list = await asyncio.to_thread(get_data_sync)

        docker_status_map: Dict[str, Dict] = {
            status_info['repo_full_name']: status_info
            for status_info in all_docker_statuses_list
        }

        for repo_config in configured_repos:
            repo_full_name = repo_config.repo_full_name
            docker_status = docker_status_map.get(repo_full_name)

            if docker_status:
                final_status_list.append(docker_status)
            else:
                final_status_list.append({
                    "repo_full_name": repo_full_name, "running": 0, "stopped": 0, "paused": 0,
                    "memory_usage_mb": 0.0, "cpu_usage_percent": 0.0, "containers": []
                })
        return final_status_list
    except Exception as e:
        logger.error(f"Controller: Error fetching container status for user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to retrieve repository/container status information.") from e


async def get_container_runtime_config_logic(
    repo_full_name: str,
    user_id: PyObjectId,
    repo: ContainerRuntimeConfigRepository,
) -> Optional[ContainerRuntimeConfig]: # Return DB Model or None
    """Controller logic to get runtime config."""
    try:
        # Run synchronous DB call in thread
        config = await asyncio.to_thread(repo.get_by_repo_and_user, repo_full_name=repo_full_name, user_id=user_id)
        return config
    except Exception as e:
        logger.error(f"Controller: Error fetching runtime config for {repo_full_name}, user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to retrieve container runtime configuration.") from e

async def save_container_runtime_config_logic(
    runtime_config_in: ContainerRuntimeConfig, # Expect full DB model (view validates/converts)
    repo_full_name: str, # Already validated by view
    user_id: PyObjectId, # Already set by view
    repo: ContainerRuntimeConfigRepository,
) -> ContainerRuntimeConfig: # Return DB Model
    """Controller logic to save runtime config."""
    runtime_config_in.user_id = user_id # Ensure user_id is set
    runtime_config_in.repo_full_name = repo_full_name # Ensure repo_name is set
    try:
        # Run synchronous DB call in thread
        saved_config = await asyncio.to_thread(repo.upsert, config=runtime_config_in)
        return saved_config
    except Exception as e:
        logger.error(f"Controller: Error saving runtime config for {repo_full_name}, user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to save container runtime configuration.") from e

async def start_container_logic(container_id: str):
    """Controller logic to start a container."""
    try:
        # Run synchronous Docker call in thread
        success = await asyncio.to_thread(docker_service.start_container, container_id)
        if not success:
            # Raise specific error for view to handle
            raise ValueError(f"Container {container_id} not found or failed to start.")
    except DockerException as e:
        logger.error(f"Controller: Docker error starting container {container_id}: {e}", exc_info=True)
        raise RuntimeError(f"Docker error: {e}") from e # Raise generic runtime error
    except Exception as e:
        logger.error(f"Controller: Unexpected error starting container {container_id}: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred.") from e

async def stop_container_logic(container_id: str):
    """Controller logic to stop a container."""
    try:
        success = await asyncio.to_thread(docker_service.stop_container, container_id)
        if not success:
            raise ValueError(f"Container {container_id} not found or failed to stop.")
    except DockerException as e:
        logger.error(f"Controller: Docker error stopping container {container_id}: {e}", exc_info=True)
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(f"Controller: Unexpected error stopping container {container_id}: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred.") from e

async def remove_container_logic(container_id: str):
    """Controller logic to remove a container."""
    try:
        success = await asyncio.to_thread(docker_service.remove_container, container_id, force=True)
        if not success:
            raise ValueError(f"Container {container_id} not found or failed to remove.")
    except DockerException as e:
        logger.error(f"Controller: Docker error removing container {container_id}: {e}", exc_info=True)
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(f"Controller: Unexpected error removing container {container_id}: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred.") from e

async def get_container_logs_logic(container_id: str, tail: int) -> str:
    """Controller logic to get container logs."""
    try:
        # Run synchronous Docker call in thread
        logs = await asyncio.to_thread(docker_service.get_container_logs, container_id, tail=tail)
        return logs
    except DockerNotFound as e: # Catch specific Docker NotFound error
        logger.warning(f"Controller: Logs requested for non-existent container {container_id}: {e}")
        raise ValueError(str(e)) # Raise ValueError for view to handle as 404
    except DockerException as e:
        logger.error(f"Controller: Docker error getting logs for container {container_id}: {e}", exc_info=True)
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(f"Controller: Unexpected error getting logs for container {container_id}: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred while fetching logs.") from e

async def scale_repository_logic(
    repo_full_name: str,
    desired_instances: int,
    user_id: PyObjectId,
    config_repo: ContainerRuntimeConfigRepository,
    build_status_repo: BuildStatusRepository,
) -> Dict[str, int]: # Return dict matching ScaleResponseView structure
    """Controller logic to scale repository instances."""
    try:
        # Run synchronous DB calls in thread
        def get_data_sync():
            _runtime_config = config_repo.get_by_repo_and_user(repo_full_name=repo_full_name, user_id=user_id)
            _latest_build = build_status_repo.find_latest_successful_build(repo_full_name=repo_full_name, user_id=user_id)
            return _runtime_config, _latest_build

        runtime_config, latest_build = await asyncio.to_thread(get_data_sync)

        if not runtime_config:
            raise ValueError(f"Runtime configuration not found for {repo_full_name}.")
        if not latest_build or not latest_build.image_tag:
             raise ValueError(f"No successful build with an image tag found for {repo_full_name}.")

        image_tag = latest_build.image_tag

        # Run synchronous Docker call in thread
        scale_result = await asyncio.to_thread(
            docker_service.scale_repository,
            repo_full_name=repo_full_name,
            image_tag=image_tag,
            runtime_config=runtime_config,
            desired_instances=desired_instances
        )
        return scale_result # Returns {"started": x, "removed": y}
    except ValueError as e: # Catch config/build not found errors
        raise e # Propagate to view
    except DockerException as e:
        logger.error(f"Controller: Docker error scaling repository {repo_full_name}: {e}", exc_info=True)
        raise RuntimeError(f"Docker scaling error: {e}") from e
    except Exception as e:
        logger.error(f"Controller: Unexpected error scaling repository {repo_full_name}: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred during scaling.") from e

async def list_docker_networks_logic() -> List[Dict[str, Any]]:
    """Controller logic to list Docker networks."""
    try:
        # Run synchronous Docker call in thread
        networks = await asyncio.to_thread(docker_service.list_networks)
        return networks
    except DockerException as e:
        logger.error(f"Controller: Docker error listing networks: {e}", exc_info=True)
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(f"Controller: Unexpected error listing networks: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred while listing networks.") from e

# WebSocket logic remains here for now, needs further refactoring into a service potentially
# ... (websocket_status_endpoint logic would be adapted if moved from view) ...
