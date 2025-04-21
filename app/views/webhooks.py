import logging
from fastapi import (
    APIRouter,
    Request,
    Header,
    Depends,
    HTTPException,
    status,
    BackgroundTasks,
)
from typing import Dict, Any, Optional

from controllers import webhooks as webhooks_controller
from repositories.repository_config_repository import (
    RepositoryConfigRepository,
    get_repo_config_repository,
)
from repositories.build_status_repository import (
    BuildStatusRepository,
    get_build_status_repository,
)
from repositories.build_log_repository import (
    BuildLogRepository,
    get_build_log_repository,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/github",
    summary="GitHub Webhook Receiver",
    description="Receives push events from GitHub webhooks, validates them using the configured secret, and schedules background processing for auto-builds.",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Dict[str, str],
)
async def github_webhook_view(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: Optional[str] = Header(None, alias="X-Hub-Signature-256"),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository),
):
    """API endpoint for receiving GitHub push event webhooks.

    Validates the incoming request's signature against the configured webhook secret.
    If valid, it parses the payload, checks if it's a push to a branch, and schedules
    a background task via the controller to handle potential auto-build triggers.
    Always returns 202 Accepted immediately if the signature is valid and basic checks pass,
    regardless of whether a build is actually triggered.

    Args:
        request (Request): The incoming request object.
        background_tasks (BackgroundTasks): FastAPI background tasks handler.
        x_hub_signature_256 (Optional[str]): The signature provided by GitHub in the header.
        repo_config_repo (RepositoryConfigRepository): Dependency for repository config access.
        build_status_repo (BuildStatusRepository): Dependency for build status access.
        build_log_repo (BuildLogRepository): Dependency for build log access.

    Returns:
        Dict[str, str]: A simple message indicating the webhook was accepted.

    Raises:
        HTTPException(400): If the signature is missing, invalid, or the payload is malformed.
        HTTPException(500): If an unexpected error occurs during initial validation or scheduling.
    """
    logger.info("View: Received request for POST /webhooks/github")
    try:
        await webhooks_controller.handle_github_push_webhook(
            request=request,
            background_tasks=background_tasks,
            x_hub_signature_256=x_hub_signature_256,
            repo_config_repo=repo_config_repo,
            build_status_repo=build_status_repo,
            build_log_repo=build_log_repo,
        )
        return {"message": "Webhook received and accepted for processing."}

    except ValueError as e:
        logger.warning(f"View: Webhook validation failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(
            f"View: Unexpected error processing webhook initial validation: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Webhook processing failed.",
        )
