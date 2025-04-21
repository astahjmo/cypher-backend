import logging
# Import BackgroundTasks
from fastapi import APIRouter, Request, Header, Depends, HTTPException, status, BackgroundTasks
from typing import Dict, Any, Optional

# Import controller function
from controllers import webhooks as webhooks_controller

# Import repositories and their dependency functions
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
from repositories.build_status_repository import BuildStatusRepository, get_build_status_repository
from repositories.build_log_repository import BuildLogRepository, get_build_log_repository

logger = logging.getLogger(__name__)

# Define router here
router = APIRouter()

# --- API Endpoints ---

@router.post(
    "/github", # Prefix will be added by main.py
    summary="GitHub Webhook Receiver",
    description="Receives push events from GitHub webhooks, validates them, and schedules build processing.",
    status_code=status.HTTP_202_ACCEPTED, # Always return 202 if validation passes
    response_model=Dict[str, str] # Simple message response
)
async def github_webhook_view(
    request: Request,
    background_tasks: BackgroundTasks, # Inject BackgroundTasks
    x_hub_signature_256: Optional[str] = Header(None, alias="X-Hub-Signature-256"), # Get signature header
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository),
):
    """
    View layer endpoint for handling GitHub push webhooks.
    Validates the request and schedules background processing.
    """
    logger.info("View: Received request for POST /webhooks/github")
    try:
        # Call the controller function, now passing background_tasks
        # The controller will now handle scheduling and return immediately if valid
        await webhooks_controller.handle_github_push_webhook(
            request=request,
            background_tasks=background_tasks, # Pass the injected object
            x_hub_signature_256=x_hub_signature_256,
            repo_config_repo=repo_config_repo,
            build_status_repo=build_status_repo,
            build_log_repo=build_log_repo,
        )
        # If the controller didn't raise an exception (like validation error), return 202
        return {"message": "Webhook received and accepted for processing."}

    except ValueError as e: # Catch signature errors, parsing errors etc. from controller
        logger.warning(f"View: Webhook validation failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException as e: # Catch other explicit HTTP exceptions
        raise e
    except Exception as e: # Catch unexpected errors during initial handling
        logger.error(f"View: Unexpected error processing webhook initial validation: {e}", exc_info=True)
        # Return 500 but keep the response simple as GitHub might not care about details
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Webhook processing failed.")
