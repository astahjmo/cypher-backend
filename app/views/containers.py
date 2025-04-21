import logging
import asyncio
import json
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Response,
    status,
    WebSocket,
    WebSocketDisconnect,
    Path,
    Request,
    Body,
    Query,
    WebSocketException,
)
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from starlette.websockets import WebSocketState
from typing import List, Set, Dict, Optional, Any, AsyncGenerator, Generator
from pymongo.database import Database
from docker.errors import DockerException, NotFound as DockerNotFound

from models.auth.db_models import User
from models.container.db_models import ContainerRuntimeConfig
from models.container.api_models import (
    ContainerDetailView,
    ContainerStatusInfoView,
    ScaleRequestView,
    ScaleResponseView,
)
from controllers import containers as containers_controller
from controllers.containers import manager  # Import manager directly for WebSocket
from repositories.container_runtime_config_repository import (
    ContainerRuntimeConfigRepository,
    get_container_runtime_config_repo,
)
from repositories.repository_config_repository import (
    RepositoryConfigRepository,
    get_repo_config_repository,
)
from repositories.build_status_repository import (
    BuildStatusRepository,
    get_build_status_repository,
)
from repositories.user_repository import UserRepository, get_user_repository
from controllers.auth import get_current_user_from_token, get_user_from_websocket_cookie
from services.docker_service import docker_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",  # Define prefix here for all routes in this view
    tags=["Containers"],
)


@router.get(
    "/containers/status",
    response_model=List[ContainerStatusInfoView],
    summary="Get Status of Configured Repositories and Containers",
    description="Retrieves a list of all configured repositories for the user and their current container status, including running instances and basic stats.",
)
async def get_container_status_list_view(
    current_user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
):
    """API endpoint to get the aggregated status of containers grouped by repository.

    Fetches repositories configured by the user and combines this with live status
    information (running/stopped counts, basic stats, instance details) from Docker.

    Args:
        current_user (User): The authenticated user, injected by dependency.
        repo_config_repo (RepositoryConfigRepository): Injected repository dependency.

    Returns:
        JSONResponse: A list of objects conforming to ContainerStatusInfoView schema.

    Raises:
        HTTPException(500): If an error occurs during status retrieval.
    """
    try:
        status_list_data = await containers_controller.get_container_status_list_logic(
            user_id=current_user.id, repo_config_repo=repo_config_repo
        )
        # Use jsonable_encoder for complex types like datetime before returning JSONResponse
        return JSONResponse(content=jsonable_encoder(status_list_data))
    except RuntimeError as e:
        logger.error(f"View: Error fetching container status list: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error fetching container status list: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve container status.",
        )


@router.get(
    "/containers/{repo_owner}/{repo_name}/config",
    response_model=Optional[
        ContainerRuntimeConfig
    ],  # Can return None if not configured
    summary="Get Container Runtime Configuration",
    description="Retrieves the saved runtime configuration (scaling, ports, env vars, etc.) for a specific repository.",
)
async def get_container_runtime_config_view(
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),
    repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
):
    """API endpoint to retrieve the runtime configuration for a repository.

    Args:
        repo_owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        current_user (User): The authenticated user.
        repo (ContainerRuntimeConfigRepository): Injected repository dependency.

    Returns:
        Optional[ContainerRuntimeConfig]: The saved configuration object, or null if none exists.

    Raises:
        HTTPException(500): If an error occurs during configuration retrieval.
    """
    repo_full_name = f"{repo_owner}/{repo_name}"
    try:
        config = await containers_controller.get_container_runtime_config_logic(
            repo_full_name=repo_full_name, user_id=current_user.id, repo=repo
        )
        # Directly return the model or None; FastAPI handles None as null JSON
        return config
    except RuntimeError as e:
        logger.error(
            f"View: Error fetching runtime config for {repo_full_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error fetching runtime config for {repo_full_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve configuration.",
        )


@router.put(
    "/containers/{repo_owner}/{repo_name}/config",
    response_model=ContainerRuntimeConfig,
    summary="Save Container Runtime Configuration",
    description="Creates or updates the runtime configuration for a specific repository.",
    status_code=status.HTTP_200_OK,  # OK for successful update/create
)
async def save_container_runtime_config_view(
    runtime_config_in: ContainerRuntimeConfig,
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),
    repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
):
    """API endpoint to save (upsert) the runtime configuration for a repository.

    Args:
        runtime_config_in (ContainerRuntimeConfig): The configuration data from the request body.
        repo_owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        current_user (User): The authenticated user.
        repo (ContainerRuntimeConfigRepository): Injected repository dependency.

    Returns:
        ContainerRuntimeConfig: The saved or updated configuration object.

    Raises:
        HTTPException(400): If the repository name in the path mismatches the payload.
        HTTPException(500): If an error occurs during saving.
    """
    repo_full_name = f"{repo_owner}/{repo_name}"
    # Basic validation: ensure path repo name matches payload repo name
    if runtime_config_in.repo_full_name != repo_full_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Repository name mismatch: Path='{repo_full_name}', Payload='{runtime_config_in.repo_full_name}'.",
        )

    try:
        saved_config = await containers_controller.save_container_runtime_config_logic(
            runtime_config_in=runtime_config_in,
            repo_full_name=repo_full_name,
            user_id=current_user.id,
            repo=repo,
        )
        return saved_config
    except RuntimeError as e:
        logger.error(
            f"View: Error saving runtime config for {repo_full_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error saving runtime config for {repo_full_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save configuration.",
        )


@router.post(
    "/containers/{container_id}/start",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Start a Container",
    description="Starts a stopped container instance by its ID.",
)
async def start_container_view(
    container_id: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),  # Auth check
):
    """API endpoint to start a specific container.

    Args:
        container_id (str): The ID of the container to start.
        current_user (User): The authenticated user (ensures endpoint is protected).

    Returns:
        Response: An empty response with status 204 No Content on success.

    Raises:
        HTTPException(404): If the container is not found.
        HTTPException(500): If a Docker error or unexpected error occurs.
    """
    # TODO: Add authorization check: Does current_user own this container?
    try:
        await containers_controller.start_container_logic(container_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as e:  # Controller raises ValueError for NotFound
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except RuntimeError as e:  # Controller raises RuntimeError for Docker/other errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error starting container {container_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.post(
    "/containers/{container_id}/stop",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Stop a Container",
    description="Stops a running container instance by its ID.",
)
async def stop_container_view(
    container_id: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),  # Auth check
):
    """API endpoint to stop a specific container.

    Args:
        container_id (str): The ID of the container to stop.
        current_user (User): The authenticated user.

    Returns:
        Response: An empty response with status 204 No Content on success.

    Raises:
        HTTPException(404): If the container is not found.
        HTTPException(500): If a Docker error or unexpected error occurs.
    """
    # TODO: Add authorization check
    try:
        await containers_controller.stop_container_logic(container_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error stopping container {container_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.delete(
    "/containers/{container_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a Container",
    description="Removes a container instance by its ID (force removes by default).",
)
async def remove_container_view(
    container_id: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),  # Auth check
):
    """API endpoint to remove a specific container.

    Args:
        container_id (str): The ID of the container to remove.
        current_user (User): The authenticated user.

    Returns:
        Response: An empty response with status 204 No Content on success.

    Raises:
        HTTPException(404): If the container is not found.
        HTTPException(500): If a Docker error or unexpected error occurs.
    """
    # TODO: Add authorization check
    try:
        await containers_controller.remove_container_logic(container_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error removing container {container_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.post(
    "/containers/{repo_owner}/{repo_name}/scale",
    response_model=ScaleResponseView,
    summary="Scale Repository Instances",
    description="Scales the number of running containers for a repository to the desired count based on the latest successful build and runtime configuration.",
)
async def scale_repository_view(
    scale_request: ScaleRequestView,
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    current_user: User = Depends(get_current_user_from_token),
    config_repo: ContainerRuntimeConfigRepository = Depends(
        get_container_runtime_config_repo
    ),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
):
    """API endpoint to scale the number of containers for a repository.

    Args:
        scale_request (ScaleRequestView): Request body containing the desired number of instances.
        repo_owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        current_user (User): The authenticated user.
        config_repo (ContainerRuntimeConfigRepository): Dependency for runtime config access.
        build_status_repo (BuildStatusRepository): Dependency for finding the latest build.

    Returns:
        ScaleResponseView: A response indicating how many containers were started and removed.

    Raises:
        HTTPException(400): If runtime config or a successful build is not found.
        HTTPException(500): If a Docker error or unexpected error occurs during scaling.
    """
    repo_full_name = f"{repo_owner}/{repo_name}"
    try:
        scale_result_dict = await containers_controller.scale_repository_logic(
            repo_full_name=repo_full_name,
            desired_instances=scale_request.desired_instances,
            user_id=current_user.id,
            config_repo=config_repo,
            build_status_repo=build_status_repo,
        )
        # Return the result directly, conforming to ScaleResponseView
        return ScaleResponseView(**scale_result_dict)
    except ValueError as e:  # Raised by controller for config/build not found
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except RuntimeError as e:  # Raised by controller for Docker/other errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error scaling repository {repo_full_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during scaling.",
        )


@router.get(
    "/networks",
    response_model=List[Dict[str, Any]],
    summary="List Docker Networks",
    description="Retrieves a list of available Docker networks on the host.",
)
async def list_docker_networks_view(
    current_user: User = Depends(get_current_user_from_token),  # Auth check
):
    """API endpoint to list available Docker networks.

    Args:
        current_user (User): The authenticated user.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing network details (id, name, driver).

    Raises:
        HTTPException(500): If a Docker error or unexpected error occurs.
    """
    # TODO: Consider if this endpoint needs more specific authorization
    try:
        networks = await containers_controller.list_docker_networks_logic()
        return networks
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(f"View: Unexpected error listing networks: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.websocket("/containers/ws/status")
async def websocket_status_endpoint(websocket: WebSocket):
    """WebSocket endpoint to stream aggregated container status updates.

    Connects clients to the ConnectionManager for broadcasting status updates
    generated by the `broadcast_status_updates` background task.

    Args:
        websocket (WebSocket): The incoming WebSocket connection.
    """
    # TODO: Implement authentication for this endpoint (e.g., using cookies like the log endpoint)
    await manager.connect(websocket)
    client_info = f"{websocket.client}"
    try:
        # Keep the connection alive, waiting for disconnect or errors
        while True:
            # You might receive keep-alive messages or control frames here
            await websocket.receive_text()  # Or receive_bytes, depending on protocol
            logger.debug(
                f"Received keep-alive or unexpected message from status WebSocket client {client_info}"
            )
    except WebSocketDisconnect:
        logger.info(f"Status WebSocket connection closed cleanly for {client_info}")
    except Exception as e:
        # Log other exceptions that might occur on receive
        logger.error(
            f"Status WebSocket receive error for {client_info}: {e}", exc_info=True
        )
    finally:
        # Ensure disconnection from the manager
        await manager.disconnect(websocket)


@router.websocket("/ws/containers/{container_id}/logs")
async def websocket_log_endpoint(
    websocket: WebSocket,
    container_id: str,
    tail: int = Query(200, ge=1, description="Number of initial lines to retrieve"),
    user_repo: UserRepository = Depends(
        get_user_repository
    ),  # Inject repo for auth check
):
    """WebSocket endpoint to stream logs for a specific container.

    Authenticates the user via the 'access_token' cookie. If authenticated,
    it streams logs from the specified container.

    Args:
        websocket (WebSocket): The incoming WebSocket connection.
        container_id (str): The ID of the container whose logs to stream.
        tail (int): The number of recent log lines to send initially.
        user_repo (UserRepository): Dependency for user data access (for auth).

    Behavior:
        - Accepts connection if authentication via cookie succeeds.
        - Streams logs line by line.
        - Closes connection if authentication fails, container not found,
          Docker errors occur, or the client disconnects.
    """
    user: Optional[User] = None
    try:
        user = await get_user_from_websocket_cookie(websocket, user_repo)
        if not user:
            # Use standard WebSocket close codes
            await websocket.close(
                code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required"
            )
            logger.warning(
                f"WebSocket log connection attempt denied for {container_id}: Invalid/missing cookie."
            )
            return

        # TODO: Add authorization check: Does 'user' have permission for 'container_id'?
        # This might involve checking if the container's repo_full_name label matches
        # a repo configured by the user, or some other ownership logic.
        # If check fails: await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Forbidden")

        await websocket.accept()
        logger.info(
            f"WebSocket log connection accepted for container {container_id} by user {user.login} (via cookie)"
        )

    except Exception as auth_exc:
        logger.error(
            f"WebSocket log authentication error for {container_id}: {auth_exc}",
            exc_info=True,
        )
        # Avoid closing if already closed
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close(
                code=status.WS_1011_INTERNAL_ERROR, reason="Authentication error"
            )
        return

    log_stream_iterator: Optional[Generator[str, None, None]] = None
    keep_running = True
    try:
        # Run the synchronous generator fetching in a thread to avoid blocking asyncio loop
        log_stream_iterator = await asyncio.to_thread(
            docker_service.get_container_logs_stream, container_id, tail
        )

        while keep_running:
            try:
                # Get next log line from the generator (running in thread)
                log_line = await asyncio.to_thread(
                    next, log_stream_iterator, StopIteration
                )

                if log_line is StopIteration:  # Generator finished
                    keep_running = False
                    break

                # Send log line to WebSocket client
                await websocket.send_text(log_line)

            except (
                StopIteration
            ):  # Generator finished (alternative way it might signal end)
                keep_running = False
                break
            except WebSocketDisconnect:
                logger.info(
                    f"WebSocket client disconnected while streaming logs for {container_id}."
                )
                keep_running = False
                break
            except Exception as loop_exc:  # Catch errors during send or next() call
                logger.error(
                    f"Error during WebSocket log streaming loop for {container_id}: {loop_exc}",
                    exc_info=True,
                )
                try:
                    # Try sending error to client before closing
                    if websocket.client_state != WebSocketState.DISCONNECTED:
                        await websocket.send_text(f"[STREAM ERROR]: {loop_exc}")
                except Exception:
                    pass  # Ignore errors sending the error message
                keep_running = False
                break

    except DockerNotFound:
        logger.warning(
            f"Container {container_id} not found for WebSocket log streaming."
        )
        try:
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.send_text(
                    f"[ERROR] Container {container_id} not found."
                )
        except Exception:
            pass
    except DockerException as e:
        logger.error(
            f"Docker error starting WebSocket log stream for {container_id}: {e}",
            exc_info=True,
        )
        try:
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.send_text(f"[ERROR] Docker error starting stream: {e}")
        except Exception:
            pass
    except (
        Exception
    ) as e:  # Catch errors during setup (e.g., getting the stream iterator)
        logger.error(
            f"Unexpected error setting up WebSocket log stream for {container_id}: {e}",
            exc_info=True,
        )
        try:
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.send_text(
                    f"[ERROR] Unexpected error setting up stream: {e}"
                )
        except Exception:
            pass
    finally:
        logger.info(
            f"Closing WebSocket log connection and resources for {container_id}"
        )
        # Clean up the generator if it has a close method (docker-py stream might not)
        if log_stream_iterator and hasattr(log_stream_iterator, "close"):
            try:
                log_stream_iterator.close()  # type: ignore
            except Exception as close_err:
                logger.warning(
                    f"Error closing docker log stream iterator for {container_id}: {close_err}"
                )
        # Ensure WebSocket is closed
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
