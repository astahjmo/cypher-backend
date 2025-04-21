import logging
import asyncio
# Add BackgroundTasks import
from fastapi import (
    APIRouter, Depends, HTTPException, status, Path, Query, Body, Request,
    Response as FastAPIResponse, BackgroundTasks
)
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse
from typing import List, Optional, Dict
from bson import ObjectId
from pydantic import ValidationError

# Import data models
from models.auth.db_models import User
from models.github.db_models import RepositoryConfig

# Import API Models
from models.build.api_models import BuildStatusView, BuildLogView, TriggerBuildRequestView, BuildDetailView

# Import controller functions
from controllers import build as build_controller

# Import repositories
from repositories.build_status_repository import BuildStatusRepository, get_build_status_repository
from repositories.build_log_repository import BuildLogRepository, get_build_log_repository
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
# Removed ContainerRuntimeConfigRepository import
# from repositories.container_runtime_config_repository import ContainerRuntimeConfigRepository, get_container_runtime_config_repo


# Import authentication dependency
from controllers.auth import get_current_user_from_token

logger = logging.getLogger(__name__)

router = APIRouter()

# --- Helper Function ---
def convert_db_model_to_view(db_model, view_model_cls):
    """Converts a DB model instance to a View model instance, handling ID conversion."""
    if not db_model: return None
    data = db_model.model_dump(by_alias=True) # Use by_alias=True for _id -> id
    # Pydantic v2 with by_alias=True should handle this automatically if alias is set in model
    # Ensure other ObjectId fields are converted to string if needed by the view model
    if 'build_id' in data and isinstance(data.get('build_id'), ObjectId): data['build_id'] = str(data['build_id'])
    try:
        # Validate data against the target view model
        return view_model_cls.model_validate(data)
    except ValidationError as e:
        logger.error(f"Validation error converting {type(db_model).__name__} to {view_model_cls.__name__}: {e.errors()}")
        logger.error(f"Data being validated: {data}")
        raise HTTPException(status_code=500, detail=f"Internal data conversion error: {e.errors()}")

# --- API Endpoints ---

@router.post(
    "/docker/{repo_owner}/{repo_name}/{branch:path}",
    summary="Trigger Docker Build",
    description="Triggers a Docker image build for the specified repository and branch.",
    response_model=Dict[str, str],
    status_code=status.HTTP_202_ACCEPTED,
)
async def trigger_docker_build_view(
    # Path parameters first
    background_tasks: BackgroundTasks,
    repo_owner: str = Path(...),
    repo_name: str = Path(...),
    branch: str = Path(...),
    payload: Optional[TriggerBuildRequestView] = Body(None),
    current_user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository),
):
    """View layer endpoint for triggering a build."""
    # Handle case where payload might be sent as null by frontend if empty
    actual_payload = payload if payload else TriggerBuildRequestView() # Create empty if None

    try:
        # Call controller function passing only necessary dependencies
        result = await build_controller.handle_docker_build_trigger(
            repo_owner=repo_owner,
            repo_name=repo_name,
            branch=branch,
            background_tasks=background_tasks,
            payload=actual_payload,
            current_user=current_user,
            repo_config_repo=repo_config_repo,
            build_status_repo=build_status_repo,
            build_log_repo=build_log_repo,
            # Removed runtime_config_repo argument
            # runtime_config_repo=runtime_config_repo,
        )
        return result
    except HTTPException as e:
        raise e
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except RuntimeError as e:
        logger.error(f"Runtime error during build trigger for {repo_owner}/{repo_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"Error in build trigger view for {repo_owner}/{repo_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to trigger build.")


@router.get(
    "/statuses",
    response_model=List[BuildStatusView],
    summary="Get Recent Build Statuses",
    description="Retrieves a list of recent build statuses for the authenticated user.",
)
async def get_build_statuses_view(
    current_user: User = Depends(get_current_user_from_token),
    repo: BuildStatusRepository = Depends(get_build_status_repository),
    limit: int = Query(20, ge=1, le=100, description="Number of recent builds to retrieve")
):
    """View layer endpoint for getting recent build statuses."""
    try:
        statuses_data = await build_controller.get_build_statuses(
            user_id=current_user.id,
            repo=repo,
            limit=limit
        )
        return [convert_db_model_to_view(status, BuildStatusView) for status in statuses_data]
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error fetching build statuses view for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve build statuses.")

@router.get(
    "",
    response_model=List[BuildStatusView],
    summary="Get All Builds",
    description="Retrieves a list of all builds accessible by the authenticated user.",
)
async def get_builds_list_view(
    current_user: User = Depends(get_current_user_from_token),
    repo: BuildStatusRepository = Depends(get_build_status_repository),
):
    """View layer endpoint for getting all builds."""
    try:
        builds_data = await build_controller.get_builds_list(
            user_id=current_user.id,
            repo=repo
        )
        return [convert_db_model_to_view(build, BuildStatusView) for build in builds_data]
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error fetching builds list view for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve builds list.")


@router.get(
    "/{build_id}",
    response_model=BuildDetailView,
    summary="Get Build Details",
    description="Retrieves detailed information for a specific build.",
)
async def get_build_detail_view(
    build_id: str = Path(..., description="ID of the build to retrieve"),
    current_user: User = Depends(get_current_user_from_token),
    repo: BuildStatusRepository = Depends(get_build_status_repository),
):
    """View layer endpoint for getting build details."""
    try:
        build_data = await build_controller.get_build_detail(
            build_id=build_id,
            user_id=current_user.id,
            repo=repo
        )
        if not build_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Build not found or not accessible.")
        return convert_db_model_to_view(build_data, BuildDetailView)
    except ValueError:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Build ID format.")
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error fetching build detail view for ID {build_id}, user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve build details.")


@router.get(
    "/{build_id}/logs",
    response_model=List[BuildLogView],
    summary="Get Historical Build Logs",
    description="Retrieves persisted historical logs for a specific build.",
)
async def get_historical_build_logs_view(
    build_id: str = Path(..., description="ID of the build whose logs to retrieve"),
    current_user: User = Depends(get_current_user_from_token),
    log_repo: BuildLogRepository = Depends(get_build_log_repository),
    status_repo: BuildStatusRepository = Depends(get_build_status_repository),
):
    """View layer endpoint for getting historical build logs."""
    try:
        logs_data = await build_controller.get_historical_build_logs(
            build_id=build_id,
            user_id=current_user.id,
            log_repo=log_repo,
            status_repo=status_repo
        )
        return [convert_db_model_to_view(log, BuildLogView) for log in logs_data]
    except ValueError:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Build not found or not accessible.")
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error fetching historical logs view for build {build_id}, user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve build logs.")


# SSE Endpoint for Live Logs
@router.get("/{build_id}/logs/stream")
async def stream_build_logs_view(
    request: Request,
    build_id: str = Path(..., description="ID of the build to stream logs for"),
    # TODO: Add authentication/authorization here
):
    """View layer endpoint for streaming build logs via SSE."""
    event_generator = build_controller.stream_build_logs(build_id, request)
    return EventSourceResponse(event_generator)
