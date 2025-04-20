import logging
import hmac
import hashlib
from fastapi import APIRouter, Request, Depends, HTTPException, BackgroundTasks, Header
from typing import Annotated
from pymongo.database import Database

# Import local modules
from config import settings
from services.db_service import get_database
# Import the repository dependency function (needed here)
from repositories.repository_config_repository import RepositoryConfigRepository
# Import the controller
from controllers import webhooks as webhooks_controller # Use alias

logger = logging.getLogger(__name__)
router = APIRouter()

# --- Dependencies ---
# Signature verification remains in the view layer as it acts on the raw request
async def verify_github_signature(
    request: Request,
    x_hub_signature_256: Annotated[str | None, Header()] = None
):
    """Dependency to verify the GitHub webhook signature."""
    if not settings.GITHUB_WEBHOOK_SECRET:
        logger.warning("GITHUB_WEBHOOK_SECRET not configured. Skipping webhook signature verification.")
        # Allow request to proceed if secret is not set, but log warning.
        # Consider raising an error if verification is mandatory.
        return

    if not x_hub_signature_256:
        logger.error("Missing X-Hub-Signature-256 header in webhook request.")
        raise HTTPException(status_code=400, detail="Missing X-Hub-Signature-256 header.")

    try:
        body = await request.body()
        secret = settings.GITHUB_WEBHOOK_SECRET.encode('utf-8')
        signature_hash = hmac.new(secret, body, hashlib.sha256).hexdigest()
        expected_signature = f"sha256={signature_hash}"

        if not hmac.compare_digest(expected_signature, x_hub_signature_256):
            logger.error(f"Webhook signature mismatch. Expected: {expected_signature}, Received: {x_hub_signature_256}")
            raise HTTPException(status_code=400, detail="Invalid webhook signature.")

        logger.info("Webhook signature verified successfully.")
    except HTTPException:
        raise # Re-raise HTTPException directly
    except Exception as e:
        logger.error(f"Error during webhook signature verification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error verifying webhook signature.")

# Dependency to get RepositoryConfigRepository instance remains here
def get_repo_config_repository(db: Database = Depends(get_database)) -> RepositoryConfigRepository:
    """Provides a RepositoryConfigRepository instance."""
    return RepositoryConfigRepository(db)

@router.post("/github", dependencies=[Depends(verify_github_signature)])
async def handle_github_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    x_github_event: Annotated[str | None, Header()] = None, # Keep header check here
):
    """
    Handles incoming GitHub webhooks, verifies signature, and delegates processing.
    """
    if x_github_event != 'push':
        logger.info(f"Received non-push event '{x_github_event}'. Ignoring.")
        return {"message": "Event ignored. Only 'push' events are processed."}

    logger.info("Received valid 'push' event. Delegating to controller.")
    response_message = await webhooks_controller.handle_github_push_event(
        request=request,
        background_tasks=background_tasks,
        repo_config_repo=repo_config_repo
    )
    return response_message
