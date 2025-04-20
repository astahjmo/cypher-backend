import logging
import uuid
import multiprocessing # Keep for now, but BackgroundTasks is preferred for async FastAPI
import json
import os # Import os for path operations
import asyncio # Import asyncio for Event
from typing import Dict, Any
from fastapi import HTTPException, BackgroundTasks, Depends # Added BackgroundTasks, Depends
from datetime import datetime, timezone # Added timezone
from bson import ObjectId

# Import local modules
from models import User, RepositoryConfig, BuildStatus, PyObjectId # Added PyObjectId
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository # Added dependency getter
from repositories.build_status_repository import BuildStatusRepository, get_build_status_repository # Added dependency getter
from repositories.build_log_repository import BuildLogRepository, get_build_log_repository # Added dependency getter
# Import the specific service function we want to run in the background
from services.docker_service import docker_service # Keep docker_service instance for now
from config import settings # Import settings for registry URL

logger = logging.getLogger(__name__)

# --- In-memory storage for SSE signaling ---
# WARNING: Simple dict not suitable for production with multiple server instances.
# Consider Redis Pub/Sub or similar for a robust solution.
sse_connections: Dict[str, asyncio.Event] = {}
TEMP_LOG_DIR = "/tmp/cypher_build_logs" # Define temp log directory

# Ensure the temporary log directory exists
os.makedirs(TEMP_LOG_DIR, exist_ok=True)


async def handle_docker_build_trigger(
    owner: str,
    repo_name: str,
    branch: str,
    user: User, # Assuming user is already authenticated and passed in
    background_tasks: BackgroundTasks, # Inject BackgroundTasks
    # Inject repositories via Depends
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository)
) -> Dict[str, str]:
    """
    Handles the request to trigger a manual Docker build.
    Creates a build status record, schedules a background task for the build,
    sets up SSE signaling, and returns a build ID.
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(f"User '{user.login}' triggered manual build for {repo_full_name} branch {branch}")

    # --- 1. Create Initial Build Status ---
    build_id: PyObjectId
    try:
        # Assuming user.id is already a PyObjectId or compatible
        initial_status = build_status_repo.create_build_status(
            user_id=user.id,
            repo_full_name=repo_full_name,
            branch=branch,
            commit_sha=None # Manual trigger might not have a specific commit initially
        )
        build_id = initial_status.id # This should be an ObjectId
        build_id_str = str(build_id)
        logger.info(f"Build {build_id_str}: Initial status created.")
    except Exception as e:
        logger.error(f"Build {build_id_str}: Failed to create initial build status for {repo_full_name}/{branch}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to initialize build status.")

    # --- 2. Define Log File Path ---
    log_file_path = os.path.join(TEMP_LOG_DIR, f"build_{build_id_str}.log")
    logger.info(f"Build {build_id_str}: Temporary log file path set to {log_file_path}")

    # --- 3. Setup SSE Signaling ---
    sse_event = asyncio.Event()
    sse_connections[build_id_str] = sse_event
    logger.info(f"Build {build_id_str}: SSE event created and stored.")

    # --- 4. Prepare Build Arguments ---
    # Ideally, get the actual clone URL from GitHub API or config if available
    repo_url = f"https://github.com/{repo_full_name}.git"
    logger.warning(f"Build {build_id_str}: Using potentially placeholder repo URL: {repo_url}")

    # Generate the image tag (similar logic to webhook controller)
    try:
        if not settings.REGISTRY_URL:
            raise ValueError("REGISTRY_URL not configured.")
        registry_host = settings.REGISTRY_URL.replace("https://", "").replace("http://", "")
        sanitized_branch = branch.replace('/', '-')
        # Manual builds might not have a commit sha initially, use a timestamp or generic tag?
        # Using 'manual' and timestamp for now. Consider passing commit if available.
        timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        build_image_tag = f"{registry_host}/{repo_full_name}:{sanitized_branch}-manual-{timestamp_tag}".lower()
        logger.info(f"Build {build_id_str}: Generated image tag: {build_image_tag}")
    except Exception as e:
        logger.error(f"Build {build_id_str}: Failed to generate image tag: {e}", exc_info=True)
        # Clean up SSE event if tag generation fails before scheduling task
        if build_id_str in sse_connections:
            del sse_connections[build_id_str]
        build_status_repo.update_build_status(build_id, {"status": "failed", "message": "Failed to generate image tag"})
        raise HTTPException(status_code=500, detail="Failed to generate image tag.")


    # --- 5. Schedule Background Task ---
    try:
        background_tasks.add_task(
            docker_service.run_build_task_and_save_logs,
            build_id=build_id, # Pass ObjectId
            log_file_path=log_file_path,
            repo_url=repo_url,
            branch=branch,
            commit_sha="manual", # Indicate manual trigger, adjust if commit is known
            build_image_tag=build_image_tag,
            build_status_repo=build_status_repo, # Pass repository instances
            build_log_repo=build_log_repo
            # Note: We don't pass the sse_event here, the SSE endpoint will access the shared dict
        )
        logger.info(f"Build {build_id_str}: Build task added to background for {repo_full_name}/{branch}")
    except Exception as e:
        logger.error(f"Build {build_id_str}: Failed to schedule build task for {repo_full_name}/{branch}: {e}", exc_info=True)
        # Clean up SSE event and update status if scheduling fails
        if build_id_str in sse_connections:
            del sse_connections[build_id_str]
        build_status_repo.update_build_status(build_id, {"status": "failed", "message": "Failed to schedule build task"})
        raise HTTPException(status_code=500, detail="Failed to schedule build task.")

    # --- 6. Return Build ID ---
    return {"build_id": build_id_str, "message": f"Build initiated for {repo_full_name}/{branch}."}

# Removed old multiprocessing logic and websocket listener
