import logging
import asyncio
import json
from fastapi import (
    APIRouter, Depends, HTTPException, Response, status, WebSocket,
    WebSocketDisconnect, Path, Request, Body, Query, WebSocketException
)
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
# Removed SSE imports
from starlette.websockets import WebSocketState
from typing import List, Set, Dict, Optional, Any, AsyncGenerator, Generator
from pymongo.database import Database
from docker.errors import DockerException, NotFound as DockerNotFound

# Import data models
from models.auth.db_models import User
from models.container.db_models import ContainerRuntimeConfig

# Import API Models
from models.container.api_models import (
    ContainerDetailView,
    ContainerStatusInfoView,
    ScaleRequestView,
    ScaleResponseView
)

# Import controller functions
from controllers import containers as containers_controller
from controllers.containers import manager, broadcast_status_updates

# Import repositories and their dependency functions
from repositories.container_runtime_config_repository import ContainerRuntimeConfigRepository, get_container_runtime_config_repo
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
from repositories.build_status_repository import BuildStatusRepository, get_build_status_repository
# Import user repo dependency function
from repositories.user_repository import UserRepository, get_user_repository

# Import authentication functions
from controllers.auth import get_current_user_from_token, get_user_from_websocket_cookie

# Import the docker service instance
from services.docker_service import docker_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["Containers"],
)

# --- HTTP API Endpoints (Signatures Corrected) ---

@router.get(
    "/containers/status",
    response_model=List[ContainerStatusInfoView],
    summary="Get Status of Configured Repositories and Containers",
    description="Retrieves a list of all configured repositories for the user and their current container status.",
)
async def get_container_status_list_view(
    current_user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository)
):
    """View layer endpoint for getting container statuses."""
    try:
        status_list_data = await containers_controller.get_container_status_list_logic(
            user_id=current_user.id,
            repo_config_repo=repo_config_repo
        )
        return JSONResponse(content=jsonable_encoder(status_list_data))
    except RuntimeError as e:
        logger.error(f"View: Error fetching container status list: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error fetching container status list: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve container status.")


@router.get(
    "/containers/{repo_owner}/{repo_name}/config",
    response_model=Optional[ContainerRuntimeConfig],
    summary="Get Container Runtime Configuration",
    description="Retrieves the saved runtime configuration for a specific repository.",
)
async def get_container_runtime_config_view(
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),
    repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo)
):
    """View layer endpoint for getting container runtime config."""
    repo_full_name = f"{repo_owner}/{repo_name}"
    try:
        config = await containers_controller.get_container_runtime_config_logic(
            repo_full_name=repo_full_name,
            user_id=current_user.id,
            repo=repo
        )
        return config
    except RuntimeError as e:
        logger.error(f"View: Error fetching runtime config for {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error fetching runtime config for {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve configuration.")


@router.put(
    "/containers/{repo_owner}/{repo_name}/config",
    response_model=ContainerRuntimeConfig,
    summary="Save Container Runtime Configuration",
    description="Creates or updates the runtime configuration for a specific repository.",
    status_code=status.HTTP_200_OK,
)
async def save_container_runtime_config_view(
    runtime_config_in: ContainerRuntimeConfig,
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),
    repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo)
):
    """View layer endpoint for saving container runtime config."""
    repo_full_name = f"{repo_owner}/{repo_name}"
    if runtime_config_in.repo_full_name != repo_full_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Repository name mismatch.")

    try:
        saved_config = await containers_controller.save_container_runtime_config_logic(
            runtime_config_in=runtime_config_in,
            repo_full_name=repo_full_name,
            user_id=current_user.id,
            repo=repo
        )
        return saved_config
    except RuntimeError as e:
        logger.error(f"View: Error saving runtime config for {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error saving runtime config for {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save configuration.")


@router.post(
    "/containers/{container_id}/start",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Start a Container",
    description="Starts a stopped container instance by its ID.",
)
async def start_container_view(
    container_id: str = Path(...),
    current_user: User = Depends(get_current_user_from_token)
):
    """View layer endpoint to start a container."""
    try:
        await containers_controller.start_container_logic(container_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error starting container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.post(
    "/containers/{container_id}/stop",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Stop a Container",
    description="Stops a running container instance by its ID.",
)
async def stop_container_view(
    container_id: str = Path(...),
    current_user: User = Depends(get_current_user_from_token)
):
    """View layer endpoint to stop a container."""
    try:
        await containers_controller.stop_container_logic(container_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error stopping container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.delete(
    "/containers/{container_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a Container",
    description="Removes a container instance by its ID (force removes by default).",
)
async def remove_container_view(
    container_id: str = Path(...),
    current_user: User = Depends(get_current_user_from_token)
):
    """View layer endpoint to remove a container."""
    try:
        await containers_controller.remove_container_logic(container_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error removing container {container_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.post(
    "/containers/{repo_owner}/{repo_name}/scale",
    response_model=ScaleResponseView,
    summary="Scale Repository Instances",
    description="Scales the number of running containers for a repository to the desired count.",
)
async def scale_repository_view(
    scale_request: ScaleRequestView,
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),
    config_repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository)
):
    """View layer endpoint to scale container instances."""
    repo_full_name = f"{repo_owner}/{repo_name}"
    try:
        scale_result_dict = await containers_controller.scale_repository_logic(
            repo_full_name=repo_full_name,
            desired_instances=scale_request.desired_instances,
            user_id=current_user.id,
            config_repo=config_repo,
            build_status_repo=build_status_repo
        )
        response_data = {
            **scale_result_dict,
            "repo_full_name": repo_full_name,
            "message": f"Scaling request processed for {repo_full_name}. Started: {scale_result_dict.get('started', 0)}, Removed: {scale_result_dict.get('removed', 0)}.",
            "requested_instances": scale_request.desired_instances
        }
        return ScaleResponseView(**response_data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error scaling repository {repo_full_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during scaling.")


@router.get(
    "/networks",
    response_model=List[Dict[str, Any]],
    summary="List Docker Networks",
    description="Retrieves a list of available Docker networks.",
)
async def list_docker_networks_view(
    current_user: User = Depends(get_current_user_from_token)
):
    """View layer endpoint to list Docker networks."""
    try:
        networks = await containers_controller.list_docker_networks_logic()
        return networks
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error listing networks: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


# --- WebSocket Endpoint for Container Status (Existing) ---
@router.websocket("/containers/ws/status")
async def websocket_status_endpoint(websocket: WebSocket):
    """WebSocket endpoint to stream container status updates."""
    # TODO: Implement authentication for this endpoint as well, likely using cookies
    await manager.connect(websocket)
    client_info = f"{websocket.client}"
    try:
        while True:
            await websocket.receive_text() # Keepalive or control messages
            logger.debug(f"Received keep-alive or unexpected message from status WebSocket client {client_info}")
    except WebSocketDisconnect:
        logger.info(f"Status WebSocket connection closed cleanly for {client_info}")
    except Exception as e:
        logger.error(f"Status WebSocket receive error for {client_info}: {e}", exc_info=True)
    finally:
        await manager.disconnect(websocket)


# --- WebSocket Endpoint for Container Logs (Using Cookie Auth) ---
@router.websocket("/ws/containers/{container_id}/logs")
async def websocket_log_endpoint(
    websocket: WebSocket,
    container_id: str,
    tail: int = Query(200, ge=1, description="Number of initial lines to retrieve"),
    # Inject UserRepository dependency manually
    user_repo: UserRepository = Depends(get_user_repository)
):
    """WebSocket endpoint to stream logs for a specific container, using cookie authentication."""
    # --- Authentication via Cookie ---
    user: Optional[User] = None
    try:
        # Use the helper function from auth controller to get user from the WebSocket's cookie
        user = await get_user_from_websocket_cookie(websocket, user_repo)

        if not user:
             await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required (cookie)")
             logger.warning(f"WebSocket log connection attempt denied for {container_id}: Invalid/missing cookie.")
             return

        # TODO: Add authorization check: Does 'user' have permission for 'container_id'?

        await websocket.accept()
        # Corrected log message to use user.login
        logger.info(f"WebSocket log connection accepted for container {container_id} by user {user.login} (via cookie)")

    except Exception as auth_exc:
        logger.error(f"WebSocket log authentication error for {container_id}: {auth_exc}", exc_info=True)
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication error")
        return

    # --- Log Streaming Logic ---
    log_stream_iterator: Optional[Generator[str, None, None]] = None
    keep_running = True
    try:
        log_stream_iterator = await asyncio.to_thread(
            docker_service.get_container_logs_stream, container_id, tail
        )

        while keep_running:
            try:
                log_line = await asyncio.to_thread(next, log_stream_iterator, StopIteration)

                if log_line is StopIteration:
                    keep_running = False
                    break

                await websocket.send_text(log_line)

            except StopIteration:
                keep_running = False
                break
            except WebSocketDisconnect:
                logger.info(f"WebSocket client disconnected while streaming logs for {container_id}.")
                keep_running = False
                break
            except Exception as loop_exc:
                logger.error(f"Error during WebSocket log streaming loop for {container_id}: {loop_exc}", exc_info=True)
                try: await websocket.send_text(f"[STREAM ERROR]: {loop_exc}")
                except Exception: pass
                keep_running = False
                break

    except DockerNotFound:
        logger.warning(f"Container {container_id} not found for WebSocket log streaming.")
        try: await websocket.send_text(f"[ERROR] Container {container_id} not found.")
        except Exception: pass
    except DockerException as e:
        logger.error(f"Docker error starting WebSocket log stream for {container_id}: {e}", exc_info=True)
        try: await websocket.send_text(f"[ERROR] Docker error starting stream: {e}")
        except Exception: pass
    except Exception as e:
        logger.error(f"Unexpected error setting up WebSocket log stream for {container_id}: {e}", exc_info=True)
        try: await websocket.send_text(f"[ERROR] Unexpected error setting up stream: {e}")
        except Exception: pass
    finally:
        logger.info(f"Closing WebSocket log connection and resources for {container_id}")
        if log_stream_iterator and hasattr(log_stream_iterator, 'close'):
             try:
                 log_stream_iterator.close() # type: ignore
             except Exception as close_err:
                 logger.warning(f"Error closing docker log stream iterator for {container_id}: {close_err}")
        if websocket.client_state != WebSocketState.DISCONNECTED:
             await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
