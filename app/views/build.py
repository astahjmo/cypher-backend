import logging
import asyncio
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Path,
    Query,
    Body,
    Request,
    Response as FastAPIResponse,
    BackgroundTasks,
)
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse
from typing import List, Optional, Dict
from bson import ObjectId
from pydantic import ValidationError

from models.auth.db_models import User
from models.github.db_models import RepositoryConfig
from models.build.api_models import (
    BuildStatusView,
    BuildLogView,
    TriggerBuildRequestView,
    BuildDetailView,
)
from controllers import build as build_controller
from repositories.build_status_repository import (
    BuildStatusRepository,
    get_build_status_repository,
)
from repositories.build_log_repository import (
    BuildLogRepository,
    get_build_log_repository,
)
from repositories.repository_config_repository import (
    RepositoryConfigRepository,
    get_repo_config_repository,
)
from controllers.auth import get_current_user_from_token

logger = logging.getLogger(__name__)

router = APIRouter()


def convert_db_model_to_view(db_model, view_model_cls):
    """Converts a database model instance to its corresponding API view model instance.

    Handles potential `_id` to `id` conversion if aliases are set correctly in the view model.
    Also converts specific known ObjectId fields (like `build_id`) to strings if present.

    Args:
        db_model: The database model instance (e.g., BuildStatus, BuildLog).
        view_model_cls: The target Pydantic view model class (e.g., BuildStatusView).

    Returns:
        An instance of the view model, or None if the input db_model is None.

    Raises:
        HTTPException(500): If validation fails during the conversion process.
    """
    if not db_model:
        return None
    # Use model_dump with by_alias=True for automatic _id -> id mapping if alias is set
    data = db_model.model_dump(by_alias=True)
    # Manually convert other known ObjectId fields if necessary for the view model
    if "build_id" in data and isinstance(data.get("build_id"), ObjectId):
        data["build_id"] = str(data["build_id"])
    try:
        return view_model_cls.model_validate(data)
    except ValidationError as e:
        logger.error(
            f"Validation error converting {type(db_model).__name__} to {view_model_cls.__name__}: {e.errors()}"
        )
        logger.error(f"Data being validated: {data}")
        # Raise HTTPException for internal conversion errors
        raise HTTPException(
            status_code=500, detail=f"Internal data conversion error: {e.errors()}"
        )


@router.post(
    "/docker/{repo_owner}/{repo_name}/{branch:path}",
    summary="Trigger Docker Build",
    description="Triggers a Docker image build for the specified repository and branch.",
    response_model=Dict[str, str],
    status_code=status.HTTP_202_ACCEPTED,
)
async def trigger_docker_build_view(
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
    """API endpoint to trigger a Docker build process.

    Accepts repository owner, name, and branch as path parameters.
    Optionally accepts a payload with build parameters (like tag version).
    Delegates the core build triggering logic to the build controller.

    Args:
        background_tasks (BackgroundTasks): FastAPI background tasks handler.
        repo_owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        branch (str): The branch to build.
        payload (Optional[TriggerBuildRequestView]): Optional request body with build options.
        current_user (User): The authenticated user initiating the build.
        repo_config_repo (RepositoryConfigRepository): Dependency for repository configuration access.
        build_status_repo (BuildStatusRepository): Dependency for build status access.
        build_log_repo (BuildLogRepository): Dependency for build log access.

    Returns:
        Dict[str, str]: A dictionary containing a confirmation message and the build ID.

    Raises:
        HTTPException(400): If input validation fails (e.g., repo config not found).
        HTTPException(500): If an internal server error occurs during build triggering.
    """
    actual_payload = payload if payload else TriggerBuildRequestView()

    try:
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
        )
        return result
    except HTTPException as e:
        # Re-raise HTTPExceptions raised by the controller or dependencies
        raise e
    except ValueError as e:
        # Convert ValueErrors (e.g., config not found) to 400 Bad Request
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except RuntimeError as e:
        # Convert RuntimeErrors (internal logic errors) to 500 Internal Server Error
        logger.error(
            f"Runtime error during build trigger for {repo_owner}/{repo_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(
            f"Error in build trigger view for {repo_owner}/{repo_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to trigger build.",
        )


@router.get(
    "/statuses",
    response_model=List[BuildStatusView],
    summary="Get Recent Build Statuses",
    description="Retrieves a list of recent build statuses for the authenticated user.",
)
async def get_build_statuses_view(
    current_user: User = Depends(get_current_user_from_token),
    repo: BuildStatusRepository = Depends(get_build_status_repository),
    limit: int = Query(
        20, ge=1, le=100, description="Number of recent builds to retrieve"
    ),
):
    """API endpoint to get recent build statuses for the current user.

    Args:
        current_user (User): The authenticated user.
        repo (BuildStatusRepository): Dependency for build status access.
        limit (int): Maximum number of build statuses to return.

    Returns:
        List[BuildStatusView]: A list of recent build statuses.

    Raises:
        HTTPException(500): If an error occurs while fetching statuses.
    """
    try:
        statuses_data = await build_controller.get_build_statuses(
            user_id=current_user.id, repo=repo, limit=limit
        )
        # Convert DB models to View models
        return [
            convert_db_model_to_view(status, BuildStatusView)
            for status in statuses_data
        ]
    except HTTPException:
        raise  # Re-raise exceptions from conversion helper
    except Exception as e:
        logger.error(
            f"Error fetching build statuses view for user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve build statuses.",
        )


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
    """API endpoint to get all build statuses for the current user.

    Args:
        current_user (User): The authenticated user.
        repo (BuildStatusRepository): Dependency for build status access.

    Returns:
        List[BuildStatusView]: A list of all build statuses for the user.

    Raises:
        HTTPException(500): If an error occurs while fetching the builds list.
    """
    try:
        builds_data = await build_controller.get_builds_list(
            user_id=current_user.id, repo=repo
        )
        return [
            convert_db_model_to_view(build, BuildStatusView) for build in builds_data
        ]
    except HTTPException:
        raise  # Re-raise exceptions from conversion helper
    except Exception as e:
        logger.error(
            f"Error fetching builds list view for user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve builds list.",
        )


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
    """API endpoint to get detailed information about a specific build.

    Args:
        build_id (str): The ID of the build to retrieve.
        current_user (User): The authenticated user.
        repo (BuildStatusRepository): Dependency for build status access.

    Returns:
        BuildDetailView: Detailed information about the build.

    Raises:
        HTTPException(400): If the build_id format is invalid.
        HTTPException(404): If the build is not found or not accessible by the user.
        HTTPException(500): If an internal error occurs during data fetching or conversion.
    """
    try:
        build_data = await build_controller.get_build_detail(
            build_id=build_id, user_id=current_user.id, repo=repo
        )
        if not build_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Build not found or not accessible.",
            )
        # Convert DB model to View model
        return convert_db_model_to_view(build_data, BuildDetailView)
    except ValueError:
        # Raised by controller if build_id format is bad
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Build ID format."
        )
    except HTTPException:
        raise  # Re-raise 404 from above or 500 from conversion helper
    except Exception as e:
        logger.error(
            f"Error fetching build detail view for ID {build_id}, user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve build details.",
        )


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
    """API endpoint to retrieve historical logs stored for a specific build.

    Ensures the user has access to the build before fetching logs.

    Args:
        build_id (str): The ID of the build whose logs to retrieve.
        current_user (User): The authenticated user.
        log_repo (BuildLogRepository): Dependency for build log access.
        status_repo (BuildStatusRepository): Dependency for build status access (for authorization check).

    Returns:
        List[BuildLogView]: A list of historical build log entries.

    Raises:
        HTTPException(404): If the build is not found or not accessible by the user.
        HTTPException(500): If an internal error occurs during data fetching or conversion.
    """
    try:
        logs_data = await build_controller.get_historical_build_logs(
            build_id=build_id,
            user_id=current_user.id,
            log_repo=log_repo,
            status_repo=status_repo,
        )
        # Convert DB models to View models
        return [convert_db_model_to_view(log, BuildLogView) for log in logs_data]
    except ValueError:
        # Raised by controller if build not found/accessible
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Build not found or not accessible.",
        )
    except HTTPException:
        raise  # Re-raise exceptions from conversion helper
    except Exception as e:
        logger.error(
            f"Error fetching historical logs view for build {build_id}, user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve build logs.",
        )


@router.get("/{build_id}/logs/stream")
async def stream_build_logs_view(
    request: Request,
    build_id: str = Path(..., description="ID of the build to stream logs for"),
):
    """API endpoint to stream build logs in real-time using Server-Sent Events (SSE).

    Connects to the build controller's log streaming generator.
    Note: Authentication/Authorization for this endpoint needs implementation.

    Args:
        request (Request): The incoming request object.
        build_id (str): The ID of the build whose logs to stream.

    Returns:
        EventSourceResponse: An SSE response streaming log events.
    """
    # TODO: Implement authentication/authorization check here.
    #       Need to verify if the user associated with the request (e.g., via cookie/token)
    #       has permission to view logs for this build_id.
    # Example (requires user dependency):
    # current_user: User = Depends(get_current_user_from_token)
    # build_status = await status_repo.get_by_id_and_user(build_id, current_user.id)
    # if not build_status:
    #     # Return an appropriate response or raise HTTPException for SSE
    #     # Returning a simple text response might be better for SSE clients
    #     return FastAPIResponse(content="Build not found or access denied.", status_code=404)

    event_generator = build_controller.stream_build_logs(build_id, request)
    return EventSourceResponse(event_generator)
