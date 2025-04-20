import logging
import asyncio
import json
from fastapi import APIRouter, Depends, HTTPException, status, WebSocket, WebSocketDisconnect, Path, Request, Body
from starlette.websockets import WebSocketState
from typing import List, Set, Dict, Optional, Any # Added Any
from pymongo.database import Database
from docker.errors import DockerException

# Import models directly
# Use updated models including PortMapping
from models import ContainerRuntimeConfig, User, PyObjectId, RepositoryConfig, PortMapping

# Import views directly
from views.containers import ContainerStatusInfoView, ScaleRequestView, ScaleResponseView

# Import services and repositories directly
from services.docker_service import docker_service
# Import only the Repository CLASS from container_runtime_config_repository
from repositories.container_runtime_config_repository import ContainerRuntimeConfigRepository
# Import repo config repo CLASS and its dependency function
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
from services.db_service import get_database
# Import build status repo and its dependency function
from repositories.build_status_repository import BuildStatusRepository, get_build_status_repository

# Import authentication dependency directly using the correct function name
from controllers.auth import get_current_user_from_token

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1", # Changed prefix to /api/v1 for consistency
    tags=["Containers"],
    # Apply auth dependency per-route
)

# --- Dependency Function DEFINITION for ContainerRuntimeConfigRepository ---
# Keep the definition here as it depends on get_database
def get_container_runtime_config_repo(db: Database = Depends(get_database)) -> ContainerRuntimeConfigRepository:
    """FastAPI dependency to get an instance of ContainerRuntimeConfigRepository."""
    return ContainerRuntimeConfigRepository(db=db)

# BuildStatusRepository and RepositoryConfigRepository dependencies are imported from their respective files

# --- Connection Manager for WebSockets (Remains Async) ---
class ConnectionManager:
    # ... (ConnectionManager remains unchanged) ...
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

# --- Background Task (Modified to add logging) ---
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
                    final_status_list: List[ContainerStatusInfoView] = []
                    try:
                        # 1. Get status of all *existing* managed containers from Docker
                        all_docker_statuses_list = docker_service.list_managed_containers()
                        logger.debug(f"Broadcast Sync: Found {len(all_docker_statuses_list)} docker statuses.") # ADDED LOG
                        docker_status_map: Dict[str, Dict] = {
                            status_info['repo_full_name']: status_info
                            for status_info in all_docker_statuses_list
                        }

                        # 2. Get all configured repositories using the injected repo instance
                        all_configs = repo_config_repo.find_all() # Use the injected repo
                        logger.debug(f"Broadcast Sync: Found {len(all_configs)} configured repos.") # ADDED LOG
                        configured_repo_names = {config.repo_full_name for config in all_configs}

                        processed_repos = set()

                        # 3. Process repos found in Docker
                        for repo_name, docker_status in docker_status_map.items():
                            status_view = ContainerStatusInfoView(**docker_status)
                            final_status_list.append(status_view)
                            processed_repos.add(repo_name)

                        # 4. Add configured repos that weren't found in Docker
                        for repo_name in configured_repo_names:
                            if repo_name not in processed_repos:
                                status_view = ContainerStatusInfoView(
                                    repo_full_name=repo_name,
                                    running=0, stopped=0, paused=0,
                                    memory_usage_mb=0.0, cpu_usage_percent=0.0,
                                    containers=[]
                                )
                                final_status_list.append(status_view)

                        logger.debug(f"Broadcast Sync: Combined list size: {len(final_status_list)}") # ADDED LOG
                        return final_status_list
                    except Exception as e:
                        logger.error(f"Error during synchronous status fetching for broadcast: {e}", exc_info=True)
                        return [] # Return empty list on error

                # Run the synchronous data fetching in a thread
                combined_status_list = await asyncio.to_thread(get_combined_status_sync)

                # Proceed only if data fetching was successful and list is not empty
                if combined_status_list: # Check if list is not empty
                    status_list_dict = [view.model_dump(mode='json') for view in combined_status_list]
                    message = json.dumps(status_list_dict)
                    logger.debug(f"Broadcasting status update: {message[:200]}...") # Log snippet
                    await manager.broadcast(message)
                elif combined_status_list is None: # Log if fetch failed
                     logger.warning("Broadcast task: Failed to fetch combined status, skipping broadcast cycle.")
                else: # Log if list is empty
                     logger.debug("Broadcast task: Combined status list is empty, skipping broadcast.")


        except Exception as e:
            logger.error(f"Error in broadcast_status_updates task loop: {e}", exc_info=True)
        finally:
             # Wait before the next cycle
             await asyncio.sleep(5) # Increased sleep time slightly


# --- API Endpoints ---

# CORRECTED Status endpoint logic
@router.get(
    "/containers/status", # Updated route
    response_model=List[ContainerStatusInfoView],
    summary="Get Status of Configured Repositories and Containers",
    description="Retrieves a list of all configured repositories for the user and their current container status.",
    dependencies=[Depends(get_current_user_from_token)]
)
async def get_container_status_list(
    current_user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository) # Inject repo config repo
):
    """
    Fetches all configured repositories for the user and checks the status
    of any associated Docker containers.
    """
    final_status_list: List[ContainerStatusInfoView] = []
    try:
        # Use the CORRECT method name: find_by_user
        configured_repos = repo_config_repo.find_by_user(current_user.id)
        # Fetch all docker statuses once
        all_docker_statuses_list = docker_service.list_managed_containers()
        docker_status_map: Dict[str, Dict] = {
            status_info['repo_full_name']: status_info
            for status_info in all_docker_statuses_list
        }

        for repo_config in configured_repos:
            repo_full_name = repo_config.repo_full_name
            docker_status = docker_status_map.get(repo_full_name)

            if docker_status:
                # Use data from docker service if container exists
                status_view = ContainerStatusInfoView(**docker_status)
            else:
                # Create placeholder if no container exists for configured repo
                status_view = ContainerStatusInfoView(
                    repo_full_name=repo_full_name,
                    running=0,
                    stopped=0,
                    paused=0,
                    memory_usage_mb=0.0,
                    cpu_usage_percent=0.0,
                    containers=[]
                )
            final_status_list.append(status_view)

        return final_status_list

    except Exception as e:
        logger.error(f"Error fetching configured repositories/container status for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve repository/container status information."
        )


# --- Runtime Config Endpoints (Remain Async) ---
# ... (rest of the endpoints remain unchanged) ...
@router.get(
    "/containers/{repo_owner}/{repo_name}/config", # Updated route
    response_model=Optional[ContainerRuntimeConfig],
    summary="Get Container Runtime Configuration",
    description="Retrieves the saved runtime configuration for a specific repository.",
    dependencies=[Depends(get_current_user_from_token)]
)
async def get_container_runtime_config(
    repo_owner: str = Path(..., description="Owner of the repository"),
    repo_name: str = Path(..., description="Name of the repository"),
    current_user: User = Depends(get_current_user_from_token),
    repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
):
    repo_full_name = f"{repo_owner}/{repo_name}"
    try:
        config = repo.get_by_repo_and_user(repo_full_name=repo_full_name, user_id=current_user.id)
        return config
    except Exception as e:
        logger.error(f"Error fetching runtime config for {repo_full_name}, user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve container runtime configuration.")

@router.put(
    "/containers/{repo_owner}/{repo_name}/config", # Updated route
    response_model=ContainerRuntimeConfig,
    summary="Save Container Runtime Configuration",
    description="Creates or updates the runtime configuration for a specific repository.",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(get_current_user_from_token)]
)
async def save_container_runtime_config(
    runtime_config_in: ContainerRuntimeConfig, # Use the full model for input validation
    repo_owner: str = Path(..., description="Owner of the repository"),
    repo_name: str = Path(..., description="Name of the repository"),
    current_user: User = Depends(get_current_user_from_token),
    repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
):
    repo_full_name = f"{repo_owner}/{repo_name}"
    # Validate repo name consistency
    if runtime_config_in.repo_full_name != repo_full_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Repository name in path does not match repository name in request body.")

    # Set the user_id from the authenticated user
    runtime_config_in.user_id = current_user.id

    try:
        # The upsert method handles creation or update
        saved_config = repo.upsert(config=runtime_config_in)
        return saved_config
    except Exception as e:
        logger.error(f"Error saving runtime config for {repo_full_name}, user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save container runtime configuration.")

# --- Container Action Endpoints (Remain Async) ---
@router.post(
    "/containers/{container_id}/start", # Updated route
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Start a Container",
    description="Starts a stopped container instance by its ID.",
    dependencies=[Depends(get_current_user_from_token)]
)
async def start_container_endpoint(container_id: str = Path(..., description="ID of the container to start")):
    try:
        success = await asyncio.to_thread(docker_service.start_container, container_id)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Container {container_id} not found or failed to start.")
    except DockerException as e:
        logger.error(f"Docker error starting container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Docker error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error starting container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")

@router.post(
    "/containers/{container_id}/stop", # Updated route
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Stop a Container",
    description="Stops a running container instance by its ID.",
    dependencies=[Depends(get_current_user_from_token)]
)
async def stop_container_endpoint(container_id: str = Path(..., description="ID of the container to stop")):
    try:
        success = await asyncio.to_thread(docker_service.stop_container, container_id)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Container {container_id} not found or failed to stop.")
    except DockerException as e:
        logger.error(f"Docker error stopping container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Docker error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error stopping container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")

@router.delete(
    "/containers/{container_id}", # Updated route
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a Container",
    description="Removes a container instance by its ID (force removes by default).",
    dependencies=[Depends(get_current_user_from_token)]
)
async def remove_container_endpoint(container_id: str = Path(..., description="ID of the container to remove")):
    try:
        success = await asyncio.to_thread(docker_service.remove_container, container_id, force=True)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Container {container_id} not found or failed to remove.")
    except DockerException as e:
        logger.error(f"Docker error removing container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Docker error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error removing container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")

@router.post(
    "/containers/{repo_owner}/{repo_name}/scale", # Updated route
    response_model=ScaleResponseView,
    summary="Scale Repository Instances",
    description="Scales the number of running containers for a repository to the desired count.",
    dependencies=[Depends(get_current_user_from_token)]
)
async def scale_repository_endpoint(
    scale_request: ScaleRequestView,
    repo_owner: str = Path(..., description="Owner of the repository"),
    repo_name: str = Path(..., description="Name of the repository"),
    current_user: User = Depends(get_current_user_from_token),
    config_repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
):
    repo_full_name = f"{repo_owner}/{repo_name}"
    desired_instances = scale_request.desired_instances
    try:
        runtime_config = config_repo.get_by_repo_and_user(repo_full_name=repo_full_name, user_id=current_user.id)
        if not runtime_config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Runtime configuration not found for {repo_full_name}. Please configure first.")
        # Use the CORRECT method name here
        latest_build = build_status_repo.find_latest_successful_build(repo_full_name=repo_full_name, user_id=current_user.id)
        if not latest_build or not latest_build.image_tag:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No successful build with an image tag found for {repo_full_name}. Please build first.")
        image_tag = latest_build.image_tag
        scale_result = await asyncio.to_thread(
            docker_service.scale_repository,
            repo_full_name=repo_full_name,
            image_tag=image_tag,
            runtime_config=runtime_config,
            desired_instances=desired_instances
        )
        return ScaleResponseView(**scale_result)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except DockerException as e:
        logger.error(f"Docker error scaling repository {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Docker scaling error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error scaling repository {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during scaling.")

# --- New Endpoint to List Docker Networks ---
@router.get(
    "/networks",
    response_model=List[Dict[str, Any]], # Return a list of dictionaries for simplicity
    summary="List Docker Networks",
    description="Retrieves a list of available Docker networks.",
    dependencies=[Depends(get_current_user_from_token)] # Secure the endpoint
)
async def list_docker_networks():
    """Lists available Docker networks."""
    try:
        networks = await asyncio.to_thread(docker_service.list_networks)
        return networks
    except DockerException as e:
        logger.error(f"Docker error listing networks: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Docker error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error listing networks: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while listing networks.")


# --- WebSocket Endpoint (Remains Async) ---
@router.websocket("/containers/ws/status") # Updated route
async def websocket_status_endpoint(websocket: WebSocket):
    """WebSocket endpoint to stream container status updates."""
    await manager.connect(websocket)
    client_info = f"{websocket.client}"
    try:
        while True:
            await websocket.receive_text()
            logger.warning(f"Received unexpected message from WebSocket client {client_info}")
    except WebSocketDisconnect:
        logger.info(f"WebSocket connection closed cleanly for {client_info}")
    except Exception as e:
        logger.error(f"WebSocket receive error for {client_info}: {e}", exc_info=True)
    finally:
        await manager.disconnect(websocket)
